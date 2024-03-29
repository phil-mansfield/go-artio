/**********************************************************************
 * Copyright (c) 2012-2013, Douglas H. Rudd
 * All rights reserved.
 *
 * This file is part of the artio library.
 *
 * artio is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * artio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * Copies of the GNU Lesser General Public License and the GNU General
 * Public License are available in the file LICENSE, included with this
 * distribution.  If you failed to receive a copy of this file, see
 * <http://www.gnu.org/licenses/>
 **********************************************************************/

#include "artio.h"
#include "artio_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

const char grid_file_suffix = 'g';

artio_grid_file *artio_grid_file_allocate(void);
void artio_grid_file_destroy(artio_grid_file *ghandle);

const double oct_pos_offsets[8][3] = {
	{ -0.5, -0.5, -0.5 }, {  0.5, -0.5, -0.5 },
	{ -0.5,  0.5, -0.5 }, {  0.5,  0.5, -0.5 },
	{ -0.5, -0.5,  0.5 }, {  0.5, -0.5,  0.5 },
	{ -0.5,  0.5,  0.5 }, {  0.5,  0.5,  0.5 }
};

/*
 * Open grid component of the fileset
 */
int artio_fileset_open_grid(artio_fileset *handle) {
	int i;
	char filename[512];
	int first_file, last_file;
	int mode;
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	/* check that the fileset doesn't already contain a grid component */
	if ( handle->open_type & ARTIO_OPEN_GRID ||
			handle->open_mode != ARTIO_FILESET_READ ||
			handle->grid != NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}
	handle->open_type |= ARTIO_OPEN_GRID;

	ghandle = artio_grid_file_allocate();
	if ( ghandle == NULL ) {
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	/* load grid parameters from header file */
	if ( artio_parameter_get_int(handle, "num_grid_files",
				&ghandle->num_grid_files) != ARTIO_SUCCESS ||
			artio_parameter_get_int( handle, "num_grid_variables",
				&ghandle->num_grid_variables ) != ARTIO_SUCCESS ) {
		return ARTIO_ERR_GRID_DATA_NOT_FOUND;
	}

	ghandle->file_sfc_index = (int64_t *)malloc(sizeof(int64_t) * (ghandle->num_grid_files + 1));
	if ( ghandle->file_sfc_index == NULL ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	artio_parameter_get_long_array(handle, "grid_file_sfc_index",
			ghandle->num_grid_files + 1, ghandle->file_sfc_index);
	artio_parameter_get_int(handle, "grid_max_level",
				&ghandle->file_max_level);

	ghandle->octs_per_level = (int *)malloc(ghandle->file_max_level * sizeof(int));
	if ( ghandle->octs_per_level == NULL ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	ghandle->ffh = (artio_fh **)malloc(ghandle->num_grid_files * sizeof(artio_fh *));
	if ( ghandle->ffh == NULL ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	for ( i = 0; i < ghandle->num_grid_files; i++ ) {
		ghandle->ffh[i] = NULL;
	}

	first_file = artio_find_file(ghandle->file_sfc_index,
			ghandle->num_grid_files, handle->proc_sfc_begin);
	last_file = artio_find_file(ghandle->file_sfc_index,
			ghandle->num_grid_files, handle->proc_sfc_end);

#ifdef ARTIO_POSIX
	if ( first_file != last_file ) {
		fprintf(stderr,"%d: ARTIO_POSIX requires one-to-one file access! first_file = %d, last_file = %d\n",
			handle->rank, first_file, last_file );
		fprintf(stderr, "%d: %ld %ld\n", handle->rank, handle->proc_sfc_begin, handle->proc_sfc_end );
		for ( i = 0; i < ghandle->num_grid_files; i++ ) {
			fprintf(stderr, "%d: %ld\n", handle->rank, ghandle->file_sfc_index[i]);
		}
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}
#endif

	/* open files on all processes */
	for (i = 0; i < ghandle->num_grid_files; i++) {
	  sprintf(filename, "%s.%c%03d", handle->file_prefix, 
				grid_file_suffix, i);

		mode = ARTIO_MODE_READ;
		if (i >= first_file && i <= last_file) {
			mode |= ARTIO_MODE_ACCESS;
		}

		if (handle->endian_swap) {
			mode |= ARTIO_MODE_ENDIAN_SWAP;
		}

		ghandle->ffh[i] = artio_file_fopen(filename, mode, handle->context);
		if ( ghandle->ffh[i] == NULL ) {
			artio_grid_file_destroy(ghandle);
			return ARTIO_ERR_GRID_FILE_NOT_FOUND;
		}
	}

	handle->grid = ghandle;
	return ARTIO_SUCCESS;
}

int artio_fileset_add_grid(artio_fileset *handle,
		int num_grid_files, int allocation_strategy,
		int num_grid_variables,
		char ** grid_variable_labels ) {
	int64_t count;
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if ( handle->open_mode != ARTIO_FILESET_WRITE ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	if ( handle->open_type & ARTIO_OPEN_GRID) {
		return ARTIO_ERR_DATA_EXISTS;
	}
	handle->open_type |= ARTIO_OPEN_GRID;

	artio_parameter_set_int(handle, "num_grid_files", num_grid_files);
	artio_parameter_set_int(handle, "num_grid_variables", num_grid_variables);
	artio_parameter_set_string_array(handle, "grid_variable_labels",
			num_grid_variables, grid_variable_labels);

	ghandle = artio_grid_file_allocate();
	if ( ghandle == NULL ) {
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	ghandle->num_grid_files = num_grid_files;
	ghandle->allocation_strategy = allocation_strategy;
	ghandle->num_grid_variables = num_grid_variables;

	/* allocate space for root tree sizes and lists */
	ghandle->sfc_size = (int64_t *)malloc(handle->num_local_root_cells*sizeof(int64_t));
	ghandle->sfc_list = (int64_t *)malloc(handle->num_local_root_cells*sizeof(int64_t));
	if ( ghandle->sfc_size == NULL ||
			ghandle->sfc_list == NULL ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}
	ghandle->sfc_count = 0;

	handle->grid = ghandle;
	return ARTIO_SUCCESS;
}

int artio_fileset_add_grid_sfc( artio_fileset *handle, int64_t sfc,
		int root_tree_num_levels, int root_tree_num_octs ) {
	artio_grid_file *ghandle;
	int64_t size;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
#ifdef ARTIO_POSIX
		case ARTIO_ALLOC_ONE_TO_ONE:
			if ( num_grid_files != handle->num_procs ) {
				return ARTIO_ERR_INVALID_FILE_NUMBER;
			}
			for (i = 0; i < num_grid_files; i++ ) {
				ghandle->file_sfc_index[i] = handle->proc_sfc_index[i];
			}
			ghandle->file_sfc_index[num_grid_files] = handle->num_root_cells;
			break;
#endif
	}

	if (handle->open_mode != ARTIO_FILESET_WRITE ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}
	ghandle = handle->grid;

	/* check for maximum level */
	if ( root_tree_num_levels > ghandle->file_max_level ) {
		ghandle->file_max_level = root_tree_num_levels;
	}

	if ( ghandle->sfc_count >= handle->num_local_root_cells ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_INVALID_STATE;
	}

	/* compute space needed for this root tree */
	size = sizeof(float) * ghandle->num_grid_variables +
		sizeof(int) * (1 + root_tree_num_levels) +
		8*root_tree_num_octs *
		(sizeof(float)*ghandle->num_grid_variables + sizeof(int));

	ghandle->sfc_list[ghandle->sfc_count] = sfc;
	ghandle->sfc_size[ghandle->sfc_count] = size;
	ghandle->sfc_count++;

	return ARTIO_SUCCESS;
}

int artio_fileset_commit_grid( artio_fileset *handle ) {
	artio_grid_file *ghandle;
	int file_max_level, local_max_level;
	int ret;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_WRITE ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	/* communicate max file level */
#ifdef ARTIO_MPI
	MPI_Allreduce( MPI_IN_PLACE, &ghandle->file_max_level, 1,
		MPI_INT, MPI_MAX, handle->context->comm );
#endif /* ARTIO_MPI */

	if ( ghandle->file_max_level < 0 ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_INVALID_STATE;
	}

	ghandle->octs_per_level = (int *)malloc(ghandle->file_max_level * sizeof(int));
	if ( ghandle->octs_per_level == NULL ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}
	artio_parameter_set_int(handle, "grid_max_level", ghandle->file_max_level);

	/* check that root tree counts equals num_local_cells */
	if ( ghandle->sfc_count != handle->num_local_root_cells ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_INVALID_STATE;
	}

	ghandle->file_sfc_index = (int64_t*)malloc( (ghandle->num_grid_files+1)*sizeof(int64_t) );
	ghandle->ffh = (artio_fh **)malloc(ghandle->num_grid_files * sizeof(artio_fh *));
	if ( ghandle->file_sfc_index == NULL || ghandle->ffh == NULL ) {
		artio_grid_file_destroy(ghandle);
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	ret = artio_fileset_distribute_sfc_to_files(
		handle,
		ghandle->sfc_list,
		ghandle->sfc_size,
		ghandle->num_grid_files,
		ghandle->allocation_strategy,
		grid_file_suffix,
		ghandle->file_sfc_index,
		ghandle->ffh );

	if ( ret != ARTIO_SUCCESS ) {
		artio_grid_file_destroy(ghandle);
		return ret;
	}

	artio_parameter_set_long_array(handle, "grid_file_sfc_index",
			ghandle->num_grid_files + 1, ghandle->file_sfc_index);
	ghandle->sfc_count = 0;

	return ARTIO_SUCCESS;
}

artio_grid_file *artio_grid_file_allocate(void) {
	artio_grid_file *ghandle =
			(artio_grid_file *)malloc(sizeof(struct artio_grid_file_struct));
	if ( ghandle != NULL ) {
		ghandle->ffh = NULL;
		ghandle->allocation_strategy = -1;
		ghandle->num_grid_variables = -1;
		ghandle->num_grid_files = -1;
		ghandle->file_sfc_index = NULL;
		ghandle->cache_sfc_begin = -1;
		ghandle->cache_sfc_end = -1;
		ghandle->sfc_offset_table = NULL;

		ghandle->sfc_size = NULL;
		ghandle->sfc_list = NULL;
		ghandle->sfc_count = -1;

		ghandle->file_max_level = -1;
		ghandle->cur_file = -1;
		ghandle->cur_num_levels = -1;
		ghandle->cur_level = -1;
		ghandle->cur_octs = -1;
		ghandle->cur_sfc = -1;
		ghandle->octs_per_level = NULL;

		ghandle->pos_flag = 0;
		ghandle->pos_cur_level = -1;
		ghandle->next_level_size = -1;
		ghandle->cur_level_size = -1;
		ghandle->cell_size_level = 1e20;
		ghandle->next_level_pos = NULL;
		ghandle->cur_level_pos = NULL;
		ghandle->next_level_oct = -1;

		ghandle->buffer_size = artio_fh_buffer_size;
		ghandle->buffer = malloc(ghandle->buffer_size);
		if ( ghandle->buffer == NULL ) {
			free(ghandle);
			return NULL;
		}
	}
	return ghandle;
}

void artio_grid_file_destroy(artio_grid_file *ghandle) {
	int i;
	if ( ghandle == NULL ) return;

	if ( ghandle->ffh != NULL ) {
		for (i = 0; i < ghandle->num_grid_files; i++) {
			if ( ghandle->ffh[i] != NULL ) {
				artio_file_fclose(ghandle->ffh[i]);
			}
		}
		free(ghandle->ffh);
	}

	if ( ghandle->sfc_size != NULL ) free( ghandle->sfc_size );
	if ( ghandle->sfc_list != NULL ) free( ghandle->sfc_list );

	if ( ghandle->sfc_offset_table != NULL ) free(ghandle->sfc_offset_table);
	if ( ghandle->octs_per_level != NULL ) free(ghandle->octs_per_level);
	if ( ghandle->file_sfc_index != NULL ) free(ghandle->file_sfc_index);
	if ( ghandle->next_level_pos != NULL ) free(ghandle->next_level_pos);
	if ( ghandle->cur_level_pos != NULL ) free(ghandle->cur_level_pos);
	if ( ghandle->buffer != NULL ) free( ghandle->buffer );

	free(ghandle);
}

int artio_fileset_close_grid(artio_fileset *handle) {
	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if ( !(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	artio_grid_file_destroy(handle->grid);
	handle->grid = NULL;
	return ARTIO_SUCCESS;
}

int artio_grid_offset_position( artio_grid_file *ghandle, int file,
		int64_t sfc, int64_t *offset ) {
	int ret;

	if ( sfc >= ghandle->cache_sfc_begin && sfc <= ghandle->cache_sfc_end ) {
		*offset = ghandle->sfc_offset_table[sfc - ghandle->cache_sfc_begin];
	} else {
		ret = artio_file_fseek(ghandle->ffh[file],
				(sfc-ghandle->file_sfc_index[file])*sizeof(int64_t),
				ARTIO_SEEK_SET );
		if ( ret != ARTIO_SUCCESS ) return ret;

		ret = artio_file_fread(ghandle->ffh[file], offset,
				1, ARTIO_TYPE_LONG );
		if ( ret != ARTIO_SUCCESS ) return ret;
	}
	return ARTIO_SUCCESS;
}

int artio_grid_octs_in_sfc_range(artio_fileset *handle,
		int64_t start, int64_t end, int64_t *num_octs_per_sfc ) {
	int i;
	int ret;
	int file;
	int64_t sfc;
	int64_t offset, next_offset, size_offset;
	int num_oct_levels;
	int *num_octs_per_level;
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	if ( start > end || start < handle->proc_sfc_begin ||
			end > handle->proc_sfc_end ) {
		return ARTIO_ERR_INVALID_SFC_RANGE;
	}

	ghandle = handle->grid;

	/* check that we're not in the middle of a read */
	if ( ghandle->cur_sfc != -1 ) {
		return ARTIO_ERR_INVALID_STATE;
	}

	for ( i = 0; i < end-start+1; i++ ) {
		num_octs_per_sfc[i] = 0;
	}

	if ( 8*ghandle->num_grid_variables <= ghandle->file_max_level ) {
		/* we can't compute the number of octs through the offset table */
		ret = artio_grid_cache_sfc_range( handle, start, end );
		if ( ret != ARTIO_SUCCESS ) return ret;

		num_octs_per_level = (int *)malloc(ghandle->file_max_level*sizeof(int) );
		if ( num_octs_per_level == NULL ) {
			return ARTIO_ERR_MEMORY_ALLOCATION;
		}

		for ( sfc = start; sfc <= end; sfc++ ) {
			ret = artio_grid_read_root_cell_begin( handle, sfc, NULL, NULL,
					&num_oct_levels, num_octs_per_level );
			if ( ret != ARTIO_SUCCESS ) return ret;

			for ( i = 0; i < num_oct_levels; i++ ) {
				num_octs_per_sfc[sfc-start] += num_octs_per_level[i];
			}

			ret = artio_grid_read_root_cell_end( handle );
			if ( ret != ARTIO_SUCCESS ) return ret;
		}

		free( num_octs_per_level );
	} else {
		/* this version guarantees minimal memory use, but may require an additional
		 * I/O sweep through the offset table */
		file = artio_find_file(ghandle->file_sfc_index, ghandle->num_grid_files, start);

		ret = artio_grid_offset_position( ghandle, file, start, &offset );
		if ( ret != ARTIO_SUCCESS ) return ret;

		for ( sfc = start; sfc <= end; sfc++ ) {
			/* read next offset or compute end of file */
			if ( sfc < ghandle->file_sfc_index[file+1] - 1 ) {
				ret = artio_grid_offset_position( ghandle, file, sfc+1, &size_offset );
				if ( ret != ARTIO_SUCCESS ) return ret;
				next_offset = size_offset;
			} else {
				/* need to seek and ftell */
				artio_file_fseek( ghandle->ffh[file], 0, ARTIO_SEEK_END );
				artio_file_ftell( ghandle->ffh[file], &size_offset );
				file++;

				if ( sfc < end && file < ghandle->num_grid_files ) {
					ret = artio_grid_offset_position( ghandle, file, sfc+1, &next_offset );
					if ( ret != ARTIO_SUCCESS ) return ret;
				}
			}

			/* this assumes (num_levels_per_root_tree)*sizeof(int) <
			 *   size of an oct, or 8*num_variables > max_level so the
			 *   number of levels drops off in rounding to int
			 */
			num_octs_per_sfc[sfc-start] = (size_offset - offset -
					sizeof(float)*ghandle->num_grid_variables - sizeof(int) ) /
				(8*(sizeof(float)*ghandle->num_grid_variables + sizeof(int) ));
			offset = next_offset;
		}
	}

	return ARTIO_SUCCESS;
}

int artio_grid_total_octs_in_sfc_range(artio_fileset *handle,
		int64_t start, int64_t end, int64_t *num_octs_in_range ) {
	int i;
	int ret;
	int file;
	int64_t sfc;
	int64_t offset, next_offset, size_offset;
	int num_oct_levels;
	int *num_octs_per_level;
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	if ( start > end || start < handle->proc_sfc_begin ||
			end > handle->proc_sfc_end ) {
		return ARTIO_ERR_INVALID_SFC_RANGE;
	}

	ghandle = handle->grid;

	/* check that we're not in the middle of a read */
	if ( ghandle->cur_sfc != -1 ) {
		return ARTIO_ERR_INVALID_STATE;
	}

	*num_octs_in_range = 0;

	if ( 8*ghandle->num_grid_variables <= ghandle->file_max_level ) {
		/* we can't compute the number of octs through the offset table */
		ret = artio_grid_cache_sfc_range( handle, start, end );
		if ( ret != ARTIO_SUCCESS ) return ret;

		num_octs_per_level = (int *)malloc(ghandle->file_max_level*sizeof(int) );
		if ( num_octs_per_level == NULL ) {
			return ARTIO_ERR_MEMORY_ALLOCATION;
		}

		for ( sfc = start; sfc <= end; sfc++ ) {
			ret = artio_grid_read_root_cell_begin( handle, sfc, NULL, NULL,
					&num_oct_levels, num_octs_per_level );
			if ( ret != ARTIO_SUCCESS ) return ret;

			for ( i = 0; i < num_oct_levels; i++ ) {
				*num_octs_in_range += num_octs_per_level[i];
			}

			ret = artio_grid_read_root_cell_end( handle );
			if ( ret != ARTIO_SUCCESS ) return ret;
		}

		free( num_octs_per_level );
	} else {
		file = artio_find_file(ghandle->file_sfc_index, ghandle->num_grid_files, start);
		ret = artio_grid_offset_position( ghandle, file, start, &offset );
		if ( ret != ARTIO_SUCCESS ) return ret;

		for ( sfc = start; sfc <= end; sfc++ ) {
			/* read next offset or compute end of file */
			if ( sfc < ghandle->file_sfc_index[file+1] - 1 ) {
				ret = artio_grid_offset_position( ghandle, file, sfc+1, &size_offset );
				if ( ret != ARTIO_SUCCESS ) return ret;
				next_offset = size_offset;
			} else {
				/* need to seek and ftell */
				artio_file_fseek( ghandle->ffh[file], 0, ARTIO_SEEK_END );
				artio_file_ftell( ghandle->ffh[file], &size_offset );
				file++;

				if ( sfc < end && file < ghandle->num_grid_files ) {
					ret = artio_grid_offset_position( ghandle, file, sfc+1, &next_offset );
					if ( ret != ARTIO_SUCCESS ) return ret;
				}
			}

			/* this assumes (num_levels_per_root_tree)*sizeof(int) <
			 *   size of an oct, or 8*num_variables > max_level so the
			 *   number of levels drops off in rounding to int
			 */
			*num_octs_in_range += (size_offset - offset -
					sizeof(float)*ghandle->num_grid_variables - sizeof(int) ) /
				(8*(sizeof(float)*ghandle->num_grid_variables + sizeof(int) ));
			offset = next_offset;
		}
	}

	return ARTIO_SUCCESS;
}

int artio_grid_cache_sfc_range(artio_fileset *handle, int64_t start, int64_t end) {
	int i;
	int ret;
	int first_file, last_file;
	int64_t first, count, cur;
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	if ( start > end || start < handle->proc_sfc_begin ||
			end > handle->proc_sfc_end ) {
		return ARTIO_ERR_INVALID_SFC_RANGE;
	}

	ghandle = handle->grid;

	/* check if we've already cached the range */
	if ( start >= ghandle->cache_sfc_begin &&
			end <= ghandle->cache_sfc_end ) {
		return ARTIO_SUCCESS;
	}

	artio_grid_clear_sfc_cache(handle);

	first_file = artio_find_file(ghandle->file_sfc_index, ghandle->num_grid_files, start);
	last_file = artio_find_file(ghandle->file_sfc_index, ghandle->num_grid_files, end);

	ghandle->cache_sfc_begin = start;
	ghandle->cache_sfc_end = end;
	ghandle->sfc_offset_table = (int64_t *)malloc(sizeof(int64_t) * (size_t)(end - start + 1));
	if ( ghandle->sfc_offset_table == NULL ) {
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	if ( ghandle->cur_file != -1 ) {
		artio_file_detach_buffer( ghandle->ffh[ghandle->cur_file]);
		ghandle->cur_file = -1;
	}

	cur = 0;
	for (i = first_file; i <= last_file; i++) {
		first = MAX( 0, start - ghandle->file_sfc_index[i] );
		count = MIN( ghandle->file_sfc_index[i+1], end+1 )
				- MAX( start, ghandle->file_sfc_index[i]);

		artio_file_attach_buffer( ghandle->ffh[i],
			ghandle->buffer, ghandle->buffer_size );

		ret = artio_file_fseek(ghandle->ffh[i],
				sizeof(int64_t) * first, ARTIO_SEEK_SET);
		if ( ret != ARTIO_SUCCESS ) return ret;

		ret = artio_file_fread(ghandle->ffh[i],
				&ghandle->sfc_offset_table[cur],
				count, ARTIO_TYPE_LONG);
		if ( ret != ARTIO_SUCCESS ) return ret;

		artio_file_detach_buffer( ghandle->ffh[i] );
		cur += count;
	}

	return ARTIO_SUCCESS;
}

int artio_grid_clear_sfc_cache( artio_fileset *handle ) {
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if ( ghandle->sfc_offset_table != NULL ) {
		free(ghandle->sfc_offset_table);
		ghandle->sfc_offset_table = NULL;
	}

	ghandle->cache_sfc_begin = -1;
	ghandle->cache_sfc_end = -1;

	return ARTIO_SUCCESS;
}

int artio_grid_seek_to_sfc(artio_fileset *handle, int64_t sfc) {
	int64_t offset;
	artio_grid_file *ghandle;
	int file;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if ( !(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if ( handle->open_mode == ARTIO_FILESET_READ ) {
		if ( ghandle->cache_sfc_begin == -1 ||
				sfc < ghandle->cache_sfc_begin ||
				sfc > ghandle->cache_sfc_end) {
			return ARTIO_ERR_INVALID_SFC;
		}
		offset = ghandle->sfc_offset_table[sfc - ghandle->cache_sfc_begin];
	} else if ( handle->open_mode == ARTIO_FILESET_WRITE ) {
		if ( ghandle->sfc_list == NULL || ghandle->sfc_size == NULL ||
				ghandle->sfc_count >= handle->num_local_root_cells ) {
			return ARTIO_ERR_INVALID_STATE;
		}
		if ( sfc != ghandle->sfc_list[ghandle->sfc_count] ) {
			return ARTIO_ERR_INVALID_SFC;
		}
		offset = ghandle->sfc_size[ghandle->sfc_count];
		ghandle->sfc_count++;
	} else {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	file = artio_find_file(ghandle->file_sfc_index, ghandle->num_grid_files, sfc);
	if ( file != ghandle->cur_file ) {
		if ( ghandle->cur_file != -1 ) {
			artio_file_detach_buffer( ghandle->ffh[ghandle->cur_file] );
		}
		if ( ghandle->buffer_size > 0 ) {
			artio_file_attach_buffer( ghandle->ffh[file],
					ghandle->buffer, ghandle->buffer_size );
		}
		ghandle->cur_file = file;
	}

	return artio_file_fseek(ghandle->ffh[ghandle->cur_file],
			offset, ARTIO_SEEK_SET);
}

int artio_grid_write_root_cell_begin(artio_fileset *handle, int64_t sfc,
		float *variables, int num_oct_levels, int *num_octs_per_level) {
	int i;
	int ret;
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_WRITE ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if (num_oct_levels < 0 || num_oct_levels > ghandle->file_max_level) {
		return ARTIO_ERR_INVALID_OCT_LEVELS;
	}

	ret = artio_grid_seek_to_sfc(handle, sfc);
	if ( ret != ARTIO_SUCCESS ) return ret;

	ret = artio_file_fwrite(ghandle->ffh[ghandle->cur_file], variables,
			ghandle->num_grid_variables, ARTIO_TYPE_FLOAT);
	if ( ret != ARTIO_SUCCESS ) return ret;

	ret = artio_file_fwrite(ghandle->ffh[ghandle->cur_file],
			&num_oct_levels, 1, ARTIO_TYPE_INT);
	if ( ret != ARTIO_SUCCESS ) return ret;

	ret = artio_file_fwrite(ghandle->ffh[ghandle->cur_file],
			num_octs_per_level, num_oct_levels, ARTIO_TYPE_INT);
	if ( ret != ARTIO_SUCCESS ) return ret;

	for (i = 0; i < num_oct_levels; i++) {
		ghandle->octs_per_level[i] = num_octs_per_level[i];
	}

	ghandle->cur_sfc = sfc;
	ghandle->cur_num_levels = num_oct_levels;
	ghandle->cur_level = -1;
	ghandle->cur_octs = 0;

	return ARTIO_SUCCESS;
}

int artio_grid_write_root_cell_end(artio_fileset *handle) {
	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_WRITE ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	handle->grid->cur_sfc = -1;
	return ARTIO_SUCCESS;
}

int artio_grid_write_level_begin(artio_fileset *handle, int level) {
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_WRITE ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if (ghandle->cur_sfc == -1 ||
			level <= 0 || level > ghandle->cur_num_levels) {
		return ARTIO_ERR_INVALID_STATE;
	}

	ghandle->cur_level = level;
	return ARTIO_SUCCESS;
}

int artio_grid_write_level_end(artio_fileset *handle) {
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_WRITE ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if (ghandle->cur_level == -1 ||
			ghandle->cur_octs != ghandle->octs_per_level[ghandle->cur_level - 1] ) {
		return ARTIO_ERR_INVALID_STATE;
	}

	ghandle->cur_level = -1;
	ghandle->cur_octs = 0;

	return ARTIO_SUCCESS;
}

int artio_grid_write_oct(artio_fileset *handle, float *variables,
		int *cellrefined) {
	int i;
	int ret;
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_WRITE ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if (ghandle->cur_level == -1 ||
			ghandle->cur_octs >= ghandle->octs_per_level[ghandle->cur_level - 1]) {
		return ARTIO_ERR_INVALID_STATE;
	}

	/* check that no last-level octs have refined cells */
	if ( ghandle->cur_level == ghandle->cur_num_levels ) {
		for ( i = 0; i < 8; i++ ) {
			if ( cellrefined[i] ) {
				return ARTIO_ERR_INVALID_OCT_REFINED;
			}
		}
	}

	ret = artio_file_fwrite(ghandle->ffh[ghandle->cur_file],
			variables, 8 * ghandle->num_grid_variables,
			ARTIO_TYPE_FLOAT);
	if ( ret != ARTIO_SUCCESS ) return ret;

	ret = artio_file_fwrite(ghandle->ffh[ghandle->cur_file],
			cellrefined, 8, ARTIO_TYPE_INT);
	if ( ret != ARTIO_SUCCESS ) return ret;

	ghandle->cur_octs++;
	return ARTIO_SUCCESS;
}

/*
 *
 */
int artio_grid_read_root_cell_begin(artio_fileset *handle, int64_t sfc,
		double *pos, float *variables, int *num_oct_levels,
		int *num_octs_per_level) {
	int i;
	int ret;
	int coords[3];
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	ret = artio_grid_seek_to_sfc(handle, sfc);
	if ( ret != ARTIO_SUCCESS ) return ret;

	if ( variables == NULL ) {
		ret = artio_file_fseek( ghandle->ffh[ghandle->cur_file],
				ghandle->num_grid_variables*sizeof(float),
				ARTIO_SEEK_CUR );
		if ( ret != ARTIO_SUCCESS ) return ret;
	} else {
		ret = artio_file_fread(ghandle->ffh[ghandle->cur_file],
				variables, ghandle->num_grid_variables,
				ARTIO_TYPE_FLOAT);
		if ( ret != ARTIO_SUCCESS ) return ret;
	}

	ret = artio_file_fread(ghandle->ffh[ghandle->cur_file],
			num_oct_levels, 1, ARTIO_TYPE_INT);
	if ( ret != ARTIO_SUCCESS ) return ret;

	if ( *num_oct_levels > ghandle->file_max_level || *num_oct_levels < 0 ) {
		return ARTIO_ERR_INVALID_OCT_LEVELS;
	}

	if ( pos != NULL ) {
		ghandle->pos_flag = 1;

		/* compute position from sfc */
		artio_sfc_coords( handle, sfc, coords );
		for ( i = 0; i < 3; i++ ) {
			pos[i] = (double)coords[i] + 0.5;
		}

		if ( *num_oct_levels > 0 ) {
			/* compute next level position */
			if ( ghandle->next_level_pos == NULL ) {
				ghandle->next_level_pos = (double *)malloc( 3*sizeof(double) );
				if ( ghandle->next_level_pos == NULL ) {
					return ARTIO_ERR_MEMORY_ALLOCATION;
				}
				ghandle->next_level_size = 1;
			}

			for ( i = 0; i < 3; i++ ) {
				ghandle->next_level_pos[i] = pos[i];
			}
			ghandle->pos_cur_level = 0;
		} else {
			ghandle->pos_cur_level = -1;
		}
	} else {
		ghandle->pos_flag = 0;
	}

	if (*num_oct_levels > 0) {
		ret = artio_file_fread(ghandle->ffh[ghandle->cur_file],
				num_octs_per_level, *num_oct_levels,
				ARTIO_TYPE_INT);
		if ( ret != ARTIO_SUCCESS ) return ret;

		for (i = 0; i < *num_oct_levels; i++) {
			ghandle->octs_per_level[i] = num_octs_per_level[i];
		}
	}

	ghandle->cur_sfc = sfc;
	ghandle->cur_num_levels = *num_oct_levels;
	ghandle->cur_level = -1;

	return ARTIO_SUCCESS;
}

int artio_grid_read_oct(artio_fileset *handle,
		double *pos,
		float *variables,
		int *refined) {
	int i, j;
	int ret;
	int local_refined[8];
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if (ghandle->cur_level == -1 ||
			ghandle->cur_octs > ghandle->octs_per_level[ghandle->cur_level - 1] ||
			(pos != NULL && !ghandle->pos_flag )) {
		return ARTIO_ERR_INVALID_STATE;
	}

	if ( variables == NULL ) {
		ret = artio_file_fseek(ghandle->ffh[ghandle->cur_file],
				8*ghandle->num_grid_variables*sizeof(float),
				ARTIO_SEEK_CUR );
		if ( ret != ARTIO_SUCCESS ) return ret;
	} else {
		ret = artio_file_fread(ghandle->ffh[ghandle->cur_file],
				variables, 8 * ghandle->num_grid_variables,
				ARTIO_TYPE_FLOAT);
		if ( ret != ARTIO_SUCCESS ) return ret;
	}

	if ( !ghandle->pos_flag && refined == NULL ) {
		ret = artio_file_fseek(ghandle->ffh[ghandle->cur_file],
				8*sizeof(int), ARTIO_SEEK_CUR );
		if ( ret != ARTIO_SUCCESS ) return ret;
	} else {
		ret = artio_file_fread(ghandle->ffh[ghandle->cur_file],
				local_refined, 8, ARTIO_TYPE_INT);
		if ( ret != ARTIO_SUCCESS ) return ret;
	}

	if ( refined != NULL ) {
		for ( i = 0; i < 8; i++ ) {
			refined[i] = local_refined[i];
		}
	}

	if ( ghandle->pos_flag ) {
		if ( pos != NULL ) {
			for ( i = 0; i < 3; i++ ) {
				pos[i] = ghandle->cur_level_pos[3*ghandle->cur_octs + i];
			}
		}

		for ( i = 0; i < 8; i++ ) {
			if ( local_refined[i] ) {
				if ( ghandle->next_level_oct >= ghandle->next_level_size ) {
					return ARTIO_ERR_INVALID_STATE;
				}
				for ( j = 0; j < 3; j++ ) {
					ghandle->next_level_pos[3*ghandle->next_level_oct+j] =
						ghandle->cur_level_pos[3*ghandle->cur_octs + j] +
						ghandle->cell_size_level*oct_pos_offsets[i][j];
				}
				ghandle->next_level_oct++;
			}
		}
	}

	ghandle->cur_octs++;

	return ARTIO_SUCCESS;
}

/*
 * Description        Obtain data from an appointed level tree node
 */
int artio_grid_read_level_begin(artio_fileset *handle, int level) {
	int i;
	int ret;
	int64_t offset = 0;
	artio_grid_file *ghandle;
	int tmp_size;
	double *tmp_pos;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if ( ghandle->cur_sfc == -1 || level <= 0 ||
			level > ghandle->cur_num_levels ||
			(ghandle->pos_flag && ghandle->pos_cur_level != level - 1) ) {
		return ARTIO_ERR_INVALID_STATE;
	}

	if ( ghandle->pos_flag ) {
		ghandle->cell_size_level = 1.0 / (double)(1<<level);

		tmp_pos = ghandle->cur_level_pos;
		tmp_size = ghandle->cur_level_size;

		ghandle->cur_level_pos = ghandle->next_level_pos;
		ghandle->cur_level_size = ghandle->next_level_size;

		ghandle->next_level_pos = tmp_pos;
		ghandle->next_level_size = tmp_size;

		ghandle->pos_cur_level = level;

		if ( level < ghandle->cur_num_levels ) {
			/* ensure the buffer for the next level positions is large enough */
			if ( ghandle->octs_per_level[level] > ghandle->next_level_size ) {
				if ( ghandle->next_level_pos != NULL ) {
					free( ghandle->next_level_pos );
				}
				ghandle->next_level_pos = (double *)malloc( 3*ghandle->octs_per_level[level]*sizeof(double) );
				if ( ghandle->next_level_pos == NULL ) {
					return ARTIO_ERR_MEMORY_ALLOCATION;
				}
				ghandle->next_level_size = ghandle->octs_per_level[level];
			}

			ghandle->next_level_oct = 0;
		}
	}

	offset = ghandle->sfc_offset_table[ghandle->cur_sfc - ghandle->cache_sfc_begin];
	offset += sizeof(float) * ghandle->num_grid_variables + sizeof(int)
			* (ghandle->cur_num_levels + 1);
	for (i = 0; i < level - 1; i++) {
		offset += 8 * (sizeof(float) * ghandle->num_grid_variables + sizeof(int))
				* ghandle->octs_per_level[i];
	}

	ret = artio_file_fseek(ghandle->ffh[ghandle->cur_file],
			offset, ARTIO_SEEK_SET);
	if ( ret != ARTIO_SUCCESS ) return ret;

	ghandle->cur_level = level;
	ghandle->cur_octs = 0;

	return ARTIO_SUCCESS;
}

/*
 * Description        Do something at the end of each kind of read operation
 */
int artio_grid_read_level_end(artio_fileset *handle) {
	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	ghandle = handle->grid;

	if (ghandle->cur_level == -1 &&
			( ghandle->cur_level < ghandle->cur_num_levels - 1 ||
				ghandle->next_level_oct != ghandle->octs_per_level[ghandle->cur_level] ) ) {
		return ARTIO_ERR_INVALID_STATE;
	}

	ghandle->cur_level = -1;
	ghandle->cur_octs = -1;
	ghandle->next_level_oct = -1;

	return ARTIO_SUCCESS;
}

int artio_grid_read_root_cell_end(artio_fileset *handle) {
	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}
	handle->grid->cur_sfc = -1;
	handle->grid->cur_level = -1;
	handle->grid->pos_flag = 0;
	handle->grid->pos_cur_level = -1;

	return ARTIO_SUCCESS;
}

int artio_grid_read_sfc_range_levels(artio_fileset *handle,
		int64_t sfc1, int64_t sfc2,
		int min_level_to_read, int max_level_to_read,
		int options,
		artio_grid_callback callback,
		void *params ) {
	int i, j;
	int64_t sfc;
	int oct, level;
	int ret;
	int *octs_per_level = NULL;
	int refined;
	int oct_refined[8];
	int root_tree_levels;
	float *variables = NULL;
	double pos[3], cell_pos[3];

	artio_grid_file *ghandle;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	if ( ( (options & ARTIO_RETURN_CELLS) &&
				!(options & ARTIO_READ_LEAFS) &&
				!(options & ARTIO_READ_REFINED)) ||
			( (options & ARTIO_RETURN_OCTS) &&
				((options & ARTIO_READ_LEAFS) ||
				(options & ARTIO_READ_REFINED) ) &&
				!((options & ARTIO_READ_ALL) == ARTIO_READ_ALL ) ) ) {
		return ARTIO_ERR_INVALID_CELL_TYPES;
	}

	ghandle = handle->grid;

	if ((min_level_to_read < 0) || (min_level_to_read > max_level_to_read)) {
		return ARTIO_ERR_INVALID_LEVEL;
	}

	octs_per_level = (int *)malloc(ghandle->file_max_level * sizeof(int));
	variables = (float *)malloc(8*ghandle->num_grid_variables * sizeof(float));

	if ( octs_per_level == NULL || variables == NULL ) {
		if ( octs_per_level != NULL ) free(octs_per_level);
		if ( variables != NULL ) free(variables);
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	ret = artio_grid_cache_sfc_range(handle, sfc1, sfc2);
	if ( ret != ARTIO_SUCCESS ) {
		free(octs_per_level);
		free(variables);
		return ret;
	}

	for (sfc = sfc1; sfc <= sfc2; sfc++) {
		ret = artio_grid_read_root_cell_begin(handle, sfc, pos,
				variables, &root_tree_levels, octs_per_level);
		if ( ret != ARTIO_SUCCESS ) {
			free(octs_per_level);
			free(variables);
			return ret;
		}

		if (min_level_to_read == 0 &&
				((options & ARTIO_READ_REFINED && root_tree_levels > 0) ||
				(options & ARTIO_READ_LEAFS && root_tree_levels == 0)) ) {
			refined = (root_tree_levels > 0) ? 1 : 0;
			callback( sfc, 0, pos, variables, &refined, params );
		}

		for (level = MAX(min_level_to_read,1);
				level <= MIN(root_tree_levels,max_level_to_read); level++) {
			ret = artio_grid_read_level_begin(handle, level);
			if ( ret != ARTIO_SUCCESS ) {
				free(octs_per_level);
				free(variables);
				return ret;
			}

			for (oct = 0; oct < octs_per_level[level - 1]; oct++) {
				ret = artio_grid_read_oct(handle, pos, variables, oct_refined);
				if ( ret != ARTIO_SUCCESS ) {
					free(octs_per_level);
					free(variables);
					return ret;
				}

				if ( options & ARTIO_RETURN_OCTS ) {
					callback( sfc, level, pos, variables, oct_refined, params );
				} else {
					for (i = 0; i < 8; i++) {
						if ( (options & ARTIO_READ_REFINED && oct_refined[i]) ||
								(options & ARTIO_READ_LEAFS && !oct_refined[i]) ) {
							for ( j = 0; j < 3; j++ ) {
								cell_pos[j] = pos[j] + ghandle->cell_size_level*oct_pos_offsets[i][j];
							}
							callback( sfc, level, cell_pos,
									&variables[i * ghandle->num_grid_variables],
									&oct_refined[i], params );
						}
					}
				}
			}
			artio_grid_read_level_end(handle);
		}
		artio_grid_read_root_cell_end(handle);
	}

	free(variables);
	free(octs_per_level);

	artio_grid_clear_sfc_cache(handle);

	return ARTIO_SUCCESS;
}

int artio_grid_read_sfc_range(artio_fileset *handle,
		int64_t sfc1, int64_t sfc2,
		int options,
		artio_grid_callback callback,
		void *params) {

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	return artio_grid_read_sfc_range_levels( handle, sfc1, sfc2,
			0, handle->grid->file_max_level, options, callback, params );
}

int artio_grid_read_selection(artio_fileset *handle,
		artio_selection *selection, int options, artio_grid_callback callback,
		void *params ) {
	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode != ARTIO_FILESET_READ ||
			!(handle->open_type & ARTIO_OPEN_GRID) ||
			handle->grid == NULL ) {
		return ARTIO_ERR_INVALID_FILESET_MODE;
	}

	return artio_grid_read_selection_levels( handle, selection,
			0, handle->grid->file_max_level, options, callback, params );
}

int artio_grid_read_selection_levels( artio_fileset *handle,
		artio_selection *selection,
		int min_level_to_read, int max_level_to_read,
		int options,
		artio_grid_callback callback, void *params ) {
	int ret;
	int64_t start, end;

	/* loop over selected ranges */
	artio_selection_iterator_reset( selection );
	while ( artio_selection_iterator( selection,
				handle->num_root_cells,
				&start, &end ) == ARTIO_SUCCESS ) {
		ret = artio_grid_read_sfc_range_levels( handle, start, end,
				min_level_to_read, max_level_to_read, options,
				callback, params);
		if ( ret != ARTIO_SUCCESS ) return ret;
	}

	return ARTIO_SUCCESS;
}
