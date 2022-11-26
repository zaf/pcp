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
	"errors"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var (
	fsync    bool
	checksum bool
)

func main() {
	var exitCode int
	defer func() { os.Exit(exitCode) }()

	if len(os.Args) != 3 {
		log.Println("Usage", os.Args[0], "[source] [destination]")
		exitCode = 1
		return
	}
	source := os.Args[1]
	destination := os.Args[2]

	log.SetFlags(log.Lshortfile)

	src, err := os.OpenFile(source, os.O_RDONLY, 0644)
	if err != nil {
		log.Println(err)
		exitCode = 1
		return
	}
	defer src.Close()
	stat, err := src.Stat()
	if err != nil {
		log.Println(err)
		exitCode = 1
		return
	}
	if !stat.Mode().IsRegular() {
		log.Println("pcp only works on regular files")
		exitCode = 1
		return
	}
	srcMode := stat.Mode().Perm()
	srcSize := stat.Size()

	dst, err := os.OpenFile(destination, os.O_RDWR|os.O_CREATE, srcMode)
	if err != nil {
		log.Println(err)
		exitCode = 1
		return
	}
	if srcSize == 0 {
		err = dst.Close()
		if err != nil {
			log.Println(err)
			exitCode = 1
		}
		return
	}
	err = dst.Truncate(srcSize)
	if err != nil {
		dst.Close()
		log.Println(err)
		exitCode = 1
		return
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

	// Set runtime to panic instead of crashing on page faults.
	debug.SetPanicOnFault(true)

	chunk := align(srcSize / int64(threads))
	g := new(errgroup.Group)
	for i := 0; i < threads; i++ {
		var start, end int64
		start = chunk * int64(i)
		if i == threads-1 {
			end = srcSize
		} else {
			end = start + chunk
		}
		g.Go(func() error { return pcopy(src, dst, start, end) })
	}
	err = g.Wait()
	if err != nil {
		dst.Close()
		log.Println(err)
		exitCode = 1
		return
	}
	err = dst.Close()
	if err != nil {
		log.Println(err)
		exitCode = 1
		return
	}
}

// Map file chunks in memory and copy data
func pcopy(src, dst *os.File, start, end int64) (err error) {
	// Handle page faults gracefully
	defer func() {
		if e := recover(); e != nil {
			log.Fatalln(e)
		}
	}()
	s, err := unix.Mmap(int(src.Fd()), start, int(end-start), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return
	}
	defer unix.Munmap(s)
	err = unix.Madvise(s, unix.MADV_SEQUENTIAL)
	if err != nil {
		return
	}
	d, err := unix.Mmap(int(dst.Fd()), start, int(end-start), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return
	}
	n := copy(d, s)
	if int64(n) != (end - start) {
		unix.Munmap(d)
		return errors.New("short write")
	}
	if fsync {
		err = unix.Msync(d, unix.MS_SYNC)
		if err != nil {
			unix.Munmap(d)
			return
		}
	}
	if checksum && md5.Sum(s) != md5.Sum(d) {
		unix.Munmap(d)
		return errors.New("verifying data failed")
	}
	return unix.Munmap(d)
}

// Align to OS page boundaries
func align(size int64) int64 {
	pageSize := int64(os.Getpagesize())
	return (size / pageSize) * pageSize
}
