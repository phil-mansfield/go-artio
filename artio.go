package artio

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

	"unsafe"
)

type ErrorCode int

const (
	Success ErrorCode = iota
	ErrParamNotFound
	ParameterExhausted
	ErrParamInvalidLength
	ErrParamTypeMismatch
	ErrParamLengthMismatch
	ErrParamLengthInvalid
	ErrParamDuplicate
	ErrParamCorrupted
	ErrParamCorruptedMagic
	ErrStringLength

	ErrInvalidFilesetMode ErrorCode = 100 + iota
	ErrInvalidFileNumber
	ErrInvalidFileMode
	ErrInvalidSfcRange
	ErrInvalidStc
	ErrInvalidState
	ErrInvlaidSeek
	ErrInvalidOctLevels
	ErrInvalidSpecies
	ErrInvalidAllocStrategy
	ErrInvalidLevel
	ErrInvalidParameterList
	ErrInvalidDatatype
	ErrInvalidOctRefined
	ErrInvalidHandle
	ErrInvalidCellTypes
	ErrInvalidBufferSize
	ErrInvalidIndex

	ErrDataExits ErrorCode = 200 + iota
	ErrInsufficientData
	ErrFileCreate
	ErrGridDataNotFound
	ErrGridFileNotFound
	ErrParticleDataNotFound
	ErrParticleFileNotFound
	ErrIOOverflow
	ErrIOWrite
	ErrIORead
	ErrBufferExits

	SelectionExhausted ErrorCode = 300 + iota
	ErrInvalidSelection
	ErrInvalidCoordinates

	ErrMemoryAllocation ErrorCode = 400 + iota

	ErrVersionMismatch ErrorCode = 500 + iota
)

type ParameterType int
const (
	String ParameterType = iota
	Char
	Int
	Float
	Double
	Long
)


type Context struct { ptr *C.artio_context }
func (c Context) IsNull() bool { return c.ptr == (*C.artio_context)(nil) }
var NullContext = Context{ (*C.artio_context)(nil) }

type Fileset struct { ptr *C.artio_fileset }
func (c Fileset) IsNull() bool { return c.ptr == (*C.artio_fileset)(nil) }
var NullFileset = Fileset{ (*C.artio_fileset)(nil) }

func FilesetOpen(prefix string, flag int, context Context) (Fileset, error)  {
	cStr := C.CString(prefix)
	defer C.free(unsafe.Pointer(cStr))

	handle := Fileset{
		C.artio_fileset_open(cStr, C.int(flag), context.ptr),
	}

	if handle.IsNull() {
		return handle, fmt.Errorf("Prefix %s does not exist", prefix)
	} else {
		return handle, nil
	}
}

func ParameterIterate(handle Fileset) (
	key string, pType ParameterType, length int, err ErrorCode,
) {
	buf := make([]byte, 64)
	ptrKey := (*C.char)(unsafe.Pointer(&buf[0]))
	ptrPType := (*C.int)(unsafe.Pointer(&pType))
	ptrN := (*C.int)(unsafe.Pointer(&length))

	err = ErrorCode(C.artio_parameter_iterate(handle.ptr, ptrKey, ptrPType, ptrN))
	key = string(buf)

	return key, pType, length, err
}