/*
	Copyright (C) 2022, Lefteris Zafiris <zaf@fastmail.com>
	This program is free software, distributed under the terms of
	the GNU GPL v3 License. See the LICENSE file
	at the top of the source tree.
*/

/*
	Parallel file copy.

	Usage: pcp [source] [destination]

	The number of parallel threads is by default the number of available CPU threads.
	To change this set the enviroment variable PCP_THREADS with the desired number of threads:
	PCP_THREADS=4 pcp [source] [destination]

	To enable syncing of data on disk set the enviroment variable PCP_SYNC to true:
	PCP_SYNC=true pcp [source] [destination]

	To verify written data set the enviroment variable PCP_VERIFY to true:
	PCP_VERIFY=true pcp [source] [destination]
*/

package main

import (
	"crypto/md5"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

var (
	fsync    bool
	checksum bool
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalln("Usage", os.Args[0], "[source] [destination]")
	}
	source := os.Args[1]
	destination := os.Args[2]

	log.SetFlags(log.Lshortfile)

	src, err := os.OpenFile(source, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	defer src.Close()
	stat, err := src.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	if !stat.Mode().IsRegular() {
		log.Fatalln("pcp only works on regular files")
	}
	srcMode := stat.Mode().Perm()
	srcSize := stat.Size()

	dst, err := os.OpenFile(destination, os.O_RDWR|os.O_CREATE, srcMode)
	if err != nil {
		log.Fatalln(err)
	}
	if srcSize == 0 {
		err = dst.Close()
		if err != nil {
			log.Fatalln(err)
		}
		return
	}

	err = unix.Fallocate(int(dst.Fd()), 0, 0, srcSize)
	if err != nil {
		dst.Close()
		log.Fatalln(err)
	}

	if strings.ToLower(os.Getenv("PCP_SYNC")) == "true" {
		fsync = true
	}
	if strings.ToLower(os.Getenv("PCP_VERIFY")) == "true" {
		checksum = true
	}

	var threads int
	t := os.Getenv("PCP_THREADS")
	if t != "" {
		threads, err = strconv.Atoi(t)
		if err != nil {
			log.Println("PCP_THREADS:", err)
			threads = 0
		}
	}
	if threads == 0 {
		threads = runtime.NumCPU()
	}
	// Don't run parallel jobs for small files
	if srcSize < int64(256*os.Getpagesize()) {
		threads = 1
	}

	chunk := align(srcSize / int64(threads))
	wg := new(sync.WaitGroup)
	var startOffset, endOffset int64
	endOffset = chunk
	for i := 0; i < threads; i++ {
		if i == threads-1 {
			endOffset = srcSize
		}
		wg.Add(1)
		go pcopy(src, dst, startOffset, endOffset, wg)
		startOffset += chunk
		endOffset += chunk
	}
	wg.Wait()
	err = dst.Close()
	if err != nil {
		log.Fatalln(err)
	}
}

// Map file chunks in memory and copy data
func pcopy(src, dst *os.File, start, end int64, wg *sync.WaitGroup) {
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
	err = unix.Madvise(d, unix.MADV_SEQUENTIAL)
	if err != nil {
		log.Fatalln(err)
	}
	n := copy(d, s)
	if int64(n) != (end - start) {
		unix.Munmap(d)
		log.Fatalln("Short write")
	}
	if fsync {
		err = unix.Msync(d, unix.MS_SYNC)
		if err != nil {
			unix.Munmap(d)
			log.Fatalln(err)
		}
	}
	if checksum && md5.Sum(s) != md5.Sum(d) {
		unix.Munmap(d)
		log.Fatalln("Verifying data failed")
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
