package main

/*
#cgo CFLAGS: -O2 -g
#cgo LDFLAGS: -lm

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "artio.h"
*/
import "C"

import (
	"fmt"
	"os"

	"unsafe"
)

type Context struct { ptr *C.artio_context }
type Fileset struct { ptr *C.artio_fileset }

func FilesetOpen(prefix string, flag int) (Fileset, error)  {
	cStr := C.CString(prefix)
	defer C.free(unsafe.Pointer(cStr))

	fileset := C.artio_fileset_open(cStr, C.int(flag), (*C.artio_context)(nil))

	if fileset.ptr != (*C.artio_fileset)(nil) {
		return fileset, fmt.Errorf("Prefix %s does not exist", prefix)
	} else {
		return fileset, nil
	}
}

func PrintHeader(prefix string) {
	_, err := FilesetOpen(prefix, 0)
	if err != nil {
		fmt.Println("File does not exist.")
	} else {
		fmt.Println("File exists.")
	}
}

func main() {
	PrintHeader(os.Args[1])
}