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
	"runtime/debug"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	force   = flag.Bool("f", false, "Overwrite destination file if it exists.")
	fsync   = flag.Bool("s", false, "Sync file to disk after done copying data.")
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
		go mcopy(src, dst, startOffset, endOffset, wg)
		startOffset += chunk
		endOffset += chunk
	}
	wg.Wait()
	return dst.Close()
}

// Map file chunks in memory and copy data
func mcopy(src, dst *os.File, start, end int64, wg *sync.WaitGroup) {
	defer wg.Done()
	// Set runtime to panic instead of crashing on bus errors.
	debug.SetPanicOnFault(true)
	defer func() {
		if e := recover(); e != nil {
			log.Fatalln(e)
		}
	}()
	s, err := unix.Mmap(int(src.Fd()), start, int(end-start), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		log.Fatalln(err)
	}
	defer unix.Munmap(s)
	err = unix.Madvise(s, unix.MADV_SEQUENTIAL)
	if err != nil {
		log.Fatalln(err)
	}
	d, err := unix.Mmap(int(dst.Fd()), start, int(end-start), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		log.Fatalln(err)
	}
	n := copy(d, s)
	if int64(n) != (end - start) {
		unix.Munmap(d)
		log.Fatalln("Short write")
	}
	if *fsync {
		err = unix.Msync(d, unix.MS_SYNC)
		if err != nil {
			unix.Munmap(d)
			log.Fatalln(err)
		}
	}
	err = unix.Munmap(d)
	if err != nil {
		log.Fatalln(err)
	}
}

// Align to OS page boundaries
func align(size int64) int64 {
	pageSize := int64(os.Getpagesize())
	return (size / pageSize) * pageSize
}
