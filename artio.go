package artio

/*
#cgo CFLAGS: -O2 -g
#cgo LDFLAGS: -lm

#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <stdint.h>
#include "artio.h"

int64_t GetPositionsCallbackN;
int64_t GetPositionsCallbackI;

int CountCallback(
	int64_t sfc_index, int species, int subspecies, int64_t pid,
	double *primary_variables, float *secondary_variables, void *params
) {
	int64_t *counts = (int64_t *) params;
	counts[species]++;
	return 0;
}

int GetPositionsCallback(
    int64_t sfx_idx, int species, int subspecies, int64_t pid,
    double *primary_variables, float *secondary_variables, void *params
) {
    int64_t i, n;
	Vector *buf;

    buf = (Vector*) params;
    i = GetPositionsCallbackI;
	n = GetPositionsCallbackN;
    if (i >= n) { return ARTIO_ERR_INVALID_SFC_RANGE; }

    buf[i][0] = primary_variables[0];
    buf[i][1] = primary_variables[1];
    buf[i][2] = primary_variables[2];
	
    GetPositionsCallbackI++;
	
    return 0;
}
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

type OpenType int
const (
	OpenHeader OpenType = iota
	OpenParticles
	OpenGrid
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

func FilesetOpen(prefix string, flag OpenType, context Context) (Fileset, error)  {
	cStr := C.CString(prefix)
	defer C.free(unsafe.Pointer(cStr))

	handle := Fileset{
		C.artio_fileset_open(cStr, C.int(flag), context.ptr),
	}

	if handle.IsNull() {
		return handle, fmt.Errorf("ARTIO prefix %s does not exist.", prefix)
	} else {
		return handle, nil
	}
}

func (handle Fileset) Close() error {
	err := ErrorCode(C.artio_fileset_close(handle.ptr))
	if err != Success {
		return fmt.Errorf("Could not close ARTIO fileset. ErrorCode: %d", err)
	}
	return nil
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
	
	return Key{ name, pType, length }, err == Success
}

func toString(buf []byte) string {
	return string(buf[:bytes.Index(buf, []byte{0})])
}

func (handle Fileset) HasKey(name string) bool {
	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))
	return 1 == C.artio_parameter_has_key(handle.ptr, cStr)
}

func (handle Fileset) Key(name string) Key {
	if !handle.HasKey(name) {
		panic(fmt.Sprintf("Key %s not in ARTIO file.", name))
	}

	length := 0
	pType := 0
	lengthPtr := (*C.int)(unsafe.Pointer(&length))
	typePtr := (*C.int)(unsafe.Pointer(&pType))

	cStr := C.CString(name)
	defer C.free(unsafe.Pointer(cStr))

	C.artio_parameter_get_array_length(handle.ptr, cStr, lengthPtr)
	C.artio_parameter_get_type(handle.ptr, cStr, typePtr)

	return Key{ name, ParameterType(pType), length }
}

func (handle Fileset) GetString(key Key) []string {
	if key.Type != String { panic("Called GetLong on non-String key.") }

	cName := C.CString(key.Name)
	defer C.free(unsafe.Pointer(cName))

	bufValues := make([][]byte, key.length)
	ptrValues := make([]*C.char, key.length)
	for i := range bufValues {
		ptrValues[i] = (*C.char)(C.malloc(MaxStringLength))
		defer C.free(unsafe.Pointer(ptrValues[i]))
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
		for i := range bufValues {
			values[i] = C.GoString(ptrValues[i])
		}
		return values
	}
}

func (handle Fileset) GetFloat(key Key) []float32 {
	if key.Type != Float { panic("Called GetLong on non-Float key.") }

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
	if key.Type != Double { panic("Called GetLong on non-Double key.") }

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
	if key.Type != Int { panic("Called GetInt on non-Int key.") }

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
	if key.Type != Long { panic("Called GetLong on non-Long key.") }

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

func (handle Fileset) ParticleCacheSfcRange(start, end int64) error {
	err := ErrorCode(C.artio_particle_cache_sfc_range(
		handle.ptr, C.int64_t(start), C.int64_t(end),
	))

	if err != Success {
		return fmt.Errorf(
			"Could not cache ARTIO sfc range (%d, %d). ErrorCode = %d",
			start, end, err,
		)
	} else {
		return nil
	}
}

func (handle Fileset) ParticleClearSfcCache() error {
	err := ErrorCode(C.artio_particle_clear_sfc_cache(handle.ptr))
	if err != Success {
		return fmt.Errorf(
			"Could not clear ARTIO particle cache. ErrorCode = %d", err,
		)
	}
	return nil
}

func (handle Fileset) OpenParticles() error {
	err := ErrorCode(C.artio_fileset_open_particles(handle.ptr))
	if err != Success {
		return fmt.Errorf(
			"Could not open ARTIO particles. ErrorCode = %d", err,
		)
	}
	return nil
}

func (handle Fileset) CloseParticles() error {
	err := ErrorCode(C.artio_fileset_close_particles(handle.ptr))
	if err != Success {
		return fmt.Errorf(
			"Could not close ARTIO particles. ErrorCode = %d", err,
		)
	}
	return nil
}

func (handle Fileset) ParticleReadRootCellBegin(
	sfc int64, speciesCountBuf []int32,
) error {
	ptrSpeciesCounts := (*C.int)(unsafe.Pointer(&speciesCountBuf[0]))
	err := ErrorCode(C.artio_particle_read_root_cell_begin(
		handle.ptr, C.int64_t(sfc), ptrSpeciesCounts,
	))
	
	if err != Success {
		localSfc, localErr := sfc, err // GC workaround.
		return fmt.Errorf(
			"Could not read ARTIO particle sfc %d. ErrorCode = %d",
			localSfc, localErr,
		)
	}
	return nil
}

func (handle Fileset) ParticleReadRootCellEnd() error {
	err := ErrorCode(C.artio_particle_read_root_cell_end(handle.ptr))

	if err != Success {
		localErr := err // GC workaround.
		return fmt.Errorf(
			"Could not complete reading ARTIO particle sfc. ErrorCode = %d",
			localErr,
		)
	}
	return nil
}

func (handle Fileset) ParticleReadSpeciesBegin(species int) error {
	err := ErrorCode(C.artio_particle_read_species_begin(
		handle.ptr, C.int(species),
	))

	if err != Success {
		localSpecies, localErr := species, err // GC workaround.
		return fmt.Errorf(
			"Could not begin to read particle species %d. ErrCode = %d",
			localSpecies, localErr,
		)
	}

	return nil
}

func (handle Fileset) ParticleReadSpeciesEnd() error {
	err := ErrorCode(C.artio_particle_read_species_end(handle.ptr))
	if err != Success {
		return fmt.Errorf("Could not complete reading ARTIO particle species")
	}
	return nil
}

func (handle Fileset) ReadParticle(
	primary []float64, secondary []float32,
) (id, subspecies int64, err error) {
	ptrID := (*C.int64_t)(unsafe.Pointer(&id))
	ptrSubspecies := (*C.int)(unsafe.Pointer(&subspecies))
	ptrPrimary := (*C.double)(unsafe.Pointer(&primary[0]))

	dummySecondary := float32(0)
	ptrSecondary := (*C.float)(unsafe.Pointer(&dummySecondary))
	if len(secondary) > 0 {
		ptrSecondary = (*C.float)(unsafe.Pointer(&secondary[0]))
	}

	errCode := ErrorCode(C.artio_particle_read_particle(
		handle.ptr, ptrID, ptrSubspecies, ptrPrimary, ptrSecondary,
	))

	if errCode != Success {
		localErr := errCode // GC workaround.
		// If you get this error, it might be an int64 vs. int32 issue.
		return  0, 0, fmt.Errorf(
			"Count not read ARTIO particle. ErrorCode = %d", localErr,
		)
	}
	return id, subspecies, nil
}

func (h Fileset) CountInRange(sfcStart, sfcEnd int64) ([]int64, error) {
	// This needs to be done with C callbacks for performance reasons.
	counts := make([]int64, h.GetInt(h.Key("num_particle_species"))[0])
	ptrCounts := unsafe.Pointer(&counts[0])

	errCode := ErrorCode(C.artio_particle_read_sfc_range(
		h.ptr, C.int64_t(sfcStart), C.int64_t(sfcEnd),
		(C.artio_particle_callback)(C.CountCallback), ptrCounts,
	))

	if errCode != Success {
		return nil, fmt.Errorf(
			"Could not count the ARTIO particles in the sfc range (%d, %d). " +
			"Error Code: %d", sfcStart, sfcEnd, errCode,
		)
	}

	return counts, nil
}

func (h Fileset) GetPositionsAt(
	species int, sfcStart, sfcEnd int64, buf [][3]float32,
) error {
	if len(buf) == 0 {
		return nil
	}

	// This needs to be done with C callbacks for performance reasons.
	// Furthermore, the global variables and locks are due to Go 1.6's
	// new and terrible pointer passing rules.
	
	C.GetPositionsCallbackI = 0
	C.GetPositionsCallbackN = C.int64_t(len(buf))

	errCode := ErrorCode(C.artio_particle_read_sfc_range_species(
		h.ptr, C.int64_t(sfcStart), C.int64_t(sfcEnd),
		C.int(species), C.int(species),
		(C.artio_particle_callback)(C.GetPositionsCallback),
		unsafe.Pointer(&buf[0]),
	))

	if errCode != Success {
		return fmt.Errorf(
			"Could not get the ARTIO positions in the sfc range (%d, %d)." +
			"Error Code: %d", sfcStart, sfcEnd, errCode,
		)
	}

	return nil
}

func (h Fileset) GetPositions(
	species int, sfcStart, sfcEnd int64,
) ([][3]float32, error) {
	counts, err := h.CountInRange(sfcStart, sfcEnd)
	if err != nil { return nil, err }
	buf := make([][3]float32, counts[species])
	err = h.GetPositionsAt(species, sfcStart, sfcEnd, buf)
	return buf, err
}
