/* This file does not belong to the artio library. */

#include <stdint.h>

/* `CountCallback` counts the number of particles of each type in a range.
 * `params` is a zeroed int64_t array that output will be written to. */
int CountCallback(
	int64_t sfc_index, int species, int subspecies, int64_t pid,
	double *primary_variables, float *secondary_variables, void *params
) {
	int64_t *counts = (int64_t *) params;
	counts[species]++;
	return 0;
}