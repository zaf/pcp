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
*/

package main

import (
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
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
	srcSize := stat.Size()

	dst, err := os.Create(destination)
	if err != nil {
		log.Fatalln(err)
	}
	defer dst.Close()
	err = dst.Truncate(srcSize)
	if err != nil {
		log.Fatalln(err)
	}

	if srcSize == 0 {
		os.Exit(0)
	}

	var fsync bool
	if strings.ToLower(os.Getenv("PCP_SYNC")) == "true" {
		fsync = true
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

	// Set runtime to panic instead of crashing on page faults.
	debug.SetPanicOnFault(true)

	chunk := align(srcSize / int64(threads))
	wg := new(sync.WaitGroup)
	var startOffset, endOffset int64
	endOffset = chunk
	for i := 0; i < threads; i++ {
		if i == threads-1 {
			endOffset = srcSize
		}
		wg.Add(1)
		go pcopy(src, dst, startOffset, endOffset, fsync, wg)
		startOffset += chunk
		endOffset += chunk
	}
	wg.Wait()
	os.Exit(0)
}

// Map file chunks in memory and copy data
func pcopy(src, dst *os.File, start, end int64, fsync bool, wg *sync.WaitGroup) {
	defer wg.Done()
	s, err := unix.Mmap(int(src.Fd()), start, int(end-start), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		log.Fatal(err)
	}
	defer unix.Munmap(s)
	err = unix.Madvise(s, unix.MADV_SEQUENTIAL)
	if err != nil {
		log.Fatal(err)
	}
	d, err := unix.Mmap(int(dst.Fd()), start, int(end-start), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		log.Fatal(err)
	}
	defer unix.Munmap(d)

	// Handle page faults gracefully
	defer func() {
		if e := recover(); e != nil {
			log.Fatal(e)
		}
	}()
	n := copy(d, s)
	if int64(n) != (end - start) {
		log.Fatal("Short write")
	}
	if fsync {
		err = unix.Msync(d, unix.MS_SYNC)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// Align to OS page size
func align(size int64) int64 {
	pageSize := int64(os.Getpagesize())
	return (size / pageSize) * pageSize
}
