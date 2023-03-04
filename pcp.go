/*
	Copyright (C) 2022, Lefteris Zafiris <zaf@fastmail.com>
	This program is free software, distributed under the terms of
	the GNU GPL v3 License. See the LICENSE file
	at the top of the source tree.
*/

/*
	Parallel file copy.

	Usage: pcp [-fs] [-t=threads] source destination

*/

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	force   = flag.Bool("f", false, "Overwrite destination file if it exists.")
	threads = flag.Int("t", 0, "Specifies the number of threads used to copy data simultaneously.")
)

func main() {
	flag.Parse()
	var err error
	log.SetFlags(log.Lshortfile)

	args := flag.Args()
	if len(args) != 2 {
		log.Fatalln("Usage", os.Args[0], "[options] source destination")
	}

	if *threads <= 0 {
		*threads = runtime.NumCPU()
	}

	source := args[0]
	destination := args[1]
	if source == destination {
		log.Fatalln(source, "and", destination, "are the same file")
	}

	if !*force {
		_, err = os.Stat(destination)
		if !os.IsNotExist(err) {
			fmt.Printf("File %s already exists, overwrite? (y/N)", destination)
			var answer string
			fmt.Scanln(&answer)
			if strings.ToLower(answer) != "y" {
				log.Fatalln("not overwritten")
			}
		}
	}
	err = pcopy(source, destination)
	if err != nil {
		log.Fatalln(err)
	}

}

// Copy file in parallel
func pcopy(source, destination string) error {
	src, err := os.OpenFile(source, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer src.Close()
	stat, err := src.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() {
		return errors.New("pcp only works on regular files")
	}
	srcMode := stat.Mode().Perm()
	srcSize := stat.Size()

	dst, err := os.OpenFile(destination, os.O_RDWR|os.O_CREATE, srcMode)
	if err != nil {
		return err
	}
	if srcSize == 0 {
		err = dst.Close()
		if err != nil {
			return err
		}
		return nil
	}

	err = dst.Truncate(srcSize)
	if err != nil {
		dst.Close()
		return err
	}

	// Don't run parallel jobs for small files
	if srcSize < int64(256*os.Getpagesize()) {
		*threads = 1
	}

	chunk := align(srcSize / int64(*threads))
	wg := new(sync.WaitGroup)
	var startOffset, endOffset int64
	endOffset = chunk
	for i := 0; i < *threads; i++ {
		if i == *threads-1 {
			endOffset = srcSize
		}
		wg.Add(1)
		go fcopy(src, dst, startOffset, endOffset, wg)
		startOffset += chunk
		endOffset += chunk
	}
	wg.Wait()
	return dst.Close()
}

// Copy data in chunks using copy_file_range()
func fcopy(src, dst *os.File, start, end int64, wg *sync.WaitGroup) {
	defer wg.Done()
	var n, written int
	var err error
	len := end - start
	remain := len
	for remain > 0 {
		for {
			n, err = unix.CopyFileRange(int(src.Fd()), &start, int(dst.Fd()), &start, int(end-start), 0)
			if err != unix.EINTR {
				break
			}
		}
		written += n
		remain -= int64(n)
	}
	if err != nil {
		log.Fatalln(err)
	}
	if int64(written) != len {
		log.Fatalln("Short write")
	}

}

// Align to OS page boundaries
func align(size int64) int64 {
	pageSize := int64(os.Getpagesize())
	return (size / pageSize) * pageSize
}
