/* This file does not belong to the artio library. */

#include <stdint.h>
#include <artio.h>

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
    PositionBuffer *pBuf;
    int64_t i;

    pBuf = (PositionBuffer*) params;
    i = pBuf->i;
    if (i >= pBuf->n) { return ARTIO_ERR_INVALID_SFC_RANGE; }

    i++;
    pBuf->buf[i][0] = primary_variables[0];
    pBuf->buf[i][1] = primary_variables[1];
    pBuf->buf[i][2] = primary_variables[2];

    return 0;
}