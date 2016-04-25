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
	"bytes"
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

const (
	MaxStringLength = 256
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

type Key struct {
	Name string
	Type ParameterType
	length int
}

func (handle Fileset) Iterate() (key Key, ok bool) {
	buf := make([]byte, 64)
	ptrName := (*C.char)(unsafe.Pointer(&buf[0]))
	pType := ParameterType(0)
	ptrPType := (*C.int)(unsafe.Pointer(&pType))
	length := 0
	ptrLength := (*C.int)(unsafe.Pointer(&length))

	err := ErrorCode(
		C.artio_parameter_iterate(handle.ptr, ptrName, ptrPType, ptrLength),
	)
	name := toString(buf)
	
	return Key{name, pType, length}, err == Success
}

func toString(buf []byte) string {
	return string(buf[:bytes.Index(buf, []byte{0})])
}


func (handle Fileset) GetString(key Key) []string {
	cName := C.CString(key.Name)
	defer C.free(unsafe.Pointer(cName))

	bufValues := make([][]byte, key.length)
	ptrValues := make([]*C.char, key.length)
	for i := range bufValues {
		bufValues[i] = make([]byte, MaxStringLength)
		ptrValues[i] = (*C.char)(unsafe.Pointer(&bufValues[i][0]))
	}

	cLength := C.int(key.length)
	cValues := (**C.char)(unsafe.Pointer(&ptrValues[0]))

	err := ErrorCode(C.artio_parameter_get_string_array(
		handle.ptr, cName, cLength, cValues,
	))

	if err != Success {
		panic(fmt.Errorf(
			"There isn't a Float key '%s' in the given ARTIO file", key.Name,
		))
	} else {
		values := make([]string, key.length)
		for i := range bufValues { values[i] = toString(bufValues[i]) }
		return values
	}
}

func (handle Fileset) GetFloat(key Key) []float32 {
	cName := C.CString(key.Name)
	defer C.free(unsafe.Pointer(cName))
	cLength := C.int(key.length)
	values := make([]float32, key.length)
	cValues := (*C.float)(unsafe.Pointer(&values[0]))

	err := ErrorCode(C.artio_parameter_get_float_array(
		handle.ptr, cName, cLength, cValues,
	))

	if err != Success {
		panic(fmt.Errorf(
			"There isn't a Float key '%s' in the given ARTIO file", key.Name,
		))
	} else {
		return values
	}
}

func (handle Fileset) GetDouble(key Key) []float64 {
	cName := C.CString(key.Name)
	defer C.free(unsafe.Pointer(cName))
	cLength := C.int(key.length)
	values := make([]float64, key.length)
	cValues := (*C.double)(unsafe.Pointer(&values[0]))

	err := ErrorCode(C.artio_parameter_get_double_array(
		handle.ptr, cName, cLength, cValues,
	))

	if err != Success {
		panic(fmt.Errorf(
			"There isn't a Double key '%s' in the given ARTIO file", key.Name,
		))
	} else {
		return values
	}
}

func (handle Fileset) GetInt(key Key) []int32 {
	cName := C.CString(key.Name)
	defer C.free(unsafe.Pointer(cName))
	cLength := C.int(key.length)
	values := make([]int32, key.length)
	cValues := (*C.int32_t)(unsafe.Pointer(&values[0]))

	err := ErrorCode(C.artio_parameter_get_int_array(
		handle.ptr, cName, cLength, cValues,
	))

	if err != Success {
		panic(fmt.Errorf(
			"There isn't a Int key '%s' in the given ARTIO file", key.Name,
		))
	} else {
		return values
	}
}

func (handle Fileset) GetLong(key Key) []int64 {
	cName := C.CString(key.Name)
	defer C.free(unsafe.Pointer(cName))
	cLength := C.int(key.length)
	values := make([]int64, key.length)
	cValues := (*C.int64_t)(unsafe.Pointer(&values[0]))

	err := ErrorCode(C.artio_parameter_get_long_array(
		handle.ptr, cName, cLength, cValues,
	))

	if err != Success {
		panic(fmt.Errorf(
			"There isn't a Long key '%s' in the given ARTIO file", key.Name,
		))
	} else {
		return values
	}
}
