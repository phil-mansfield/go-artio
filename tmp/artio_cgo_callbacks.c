/* This file does not belong to the artio library. */

#include <stdint.h>
#include <stdio.h>
#include "artio.h"

int64_t GetPositionsCallbackN;
int64_t GetPositionsCallbackI;

/* `CountCallback` counts the number of particles of each species in a range.
 * `params` is a zeroed int64_t array that output will be written to. */
int CountCallback(
	int64_t sfc_index, int species, int subspecies, int64_t pid,
	double *primary_variables, float *secondary_variables, void *params
) {
	int64_t *counts = (int64_t *) params;
	counts[species]++;
	return 0;
}

/* `GetPositionsCallback` reads the positions in a range to a buffer.
 * `params` is a pointer to a PositionBuffer struct. */
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
