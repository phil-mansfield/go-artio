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
#include <string.h>
#include <math.h>

artio_fileset *artio_fileset_allocate( char *file_prefix, int mode,
		const artio_context *context );
void artio_fileset_destroy( artio_fileset *handle );

int artio_fh_buffer_size = ARTIO_DEFAULT_BUFFER_SIZE;

int artio_fileset_set_buffer_size( int buffer_size ) {
	if ( buffer_size < 0 ) {
		return ARTIO_ERR_INVALID_BUFFER_SIZE;
	}

	artio_fh_buffer_size = buffer_size;
	return ARTIO_SUCCESS;
}

artio_fileset *artio_fileset_open(char * file_prefix, int type, const artio_context *context) {
	artio_fh *head_fh;
	char filename[256];
	int ret;
	int64_t tmp;
	int artio_major, artio_minor;

	artio_fileset *handle =
		artio_fileset_allocate( file_prefix, ARTIO_FILESET_READ, context );
	if ( handle == NULL ) {
		return NULL;
	}

	/* open header file */
	sprintf(filename, "%s.art", handle->file_prefix);
	head_fh = artio_file_fopen(filename,
			ARTIO_MODE_READ | ARTIO_MODE_ACCESS, context);

	if ( head_fh == NULL ) {
		artio_fileset_destroy(handle);
		return NULL;
	}

	ret = artio_parameter_read(head_fh, handle->parameters );
	if ( ret != ARTIO_SUCCESS ) {
		artio_fileset_destroy(handle);
		return NULL;
	}

	/* detect parameter file endianness and propagate */
	handle->endian_swap = artio_file_get_endian_swap_tag(head_fh);
	artio_file_fclose(head_fh);

	/* check versions */
	if ( artio_parameter_get_int(handle, "ARTIO_MAJOR_VERSION", &artio_major ) ==
			ARTIO_ERR_PARAM_NOT_FOUND ) {
		/* version pre 1.0 */
		artio_major = 0;
		artio_minor = 9;
	} else {
		artio_parameter_get_int(handle, "ARTIO_MINOR_VERSION", &artio_minor );
	}

	if ( artio_major > ARTIO_MAJOR_VERSION ) {
		fprintf(stderr,"ERROR: artio file version newer than library (%u.%u vs %u.%u).\n",
			artio_major, artio_minor, ARTIO_MAJOR_VERSION, ARTIO_MINOR_VERSION );
		artio_fileset_destroy(handle);
		return NULL;
	}

	artio_parameter_get_long(handle, "num_root_cells", &handle->num_root_cells);

	/* default to sfc_type == ARTIO_SFC_HILBERT */
	if ( artio_parameter_get_int(handle, "sfc_type", &handle->sfc_type ) != ARTIO_SUCCESS ) {
		handle->sfc_type = ARTIO_SFC_HILBERT;
	}

	if ( handle->sfc_type != ARTIO_SFC_SLAB_X &&
			handle->sfc_type != ARTIO_SFC_MORTON &&
			handle->sfc_type != ARTIO_SFC_HILBERT &&
			handle->sfc_type != ARTIO_SFC_SLAB_Y &&
			handle->sfc_type != ARTIO_SFC_SLAB_Z ) {
		artio_fileset_destroy(handle);
		return NULL;
	}

	handle->nBitsPerDim = 0;
	tmp = handle->num_root_cells >> 3;
	while ( tmp ) {
		handle->nBitsPerDim++;
		tmp >>= 3;
	}
	handle->num_grid = 1<<handle->nBitsPerDim;

	/* default to accessing all sfc indices */
	handle->proc_sfc_begin = 0;
	handle->proc_sfc_end = handle->num_root_cells-1;

	/* open data files */
	if (type & ARTIO_OPEN_PARTICLES) {
		ret = artio_fileset_open_particles(handle);
		if ( ret != ARTIO_SUCCESS ) {
			artio_fileset_destroy(handle);
			return NULL;
		}
	}

	if (type & ARTIO_OPEN_GRID) {
		ret = artio_fileset_open_grid(handle);
		if ( ret != ARTIO_SUCCESS ) {
			artio_fileset_destroy(handle);
			return NULL;
		}
	}

	return handle;
}

artio_fileset *artio_fileset_create(char * file_prefix,
		int sfc_type, int64_t root_cells,
		int64_t local_root_cells,
		const artio_context *context) {
	int i;
	int tmp;
	int64_t total_local_root_cells;

	artio_fileset *handle =
		artio_fileset_allocate( file_prefix, ARTIO_FILESET_WRITE, context );
	if ( handle == NULL ) {
		return NULL;
	}

	if ( sfc_type != ARTIO_SFC_SLAB_X &&
			sfc_type != ARTIO_SFC_MORTON &&
			sfc_type != ARTIO_SFC_HILBERT &&
			sfc_type != ARTIO_SFC_SLAB_Y &&
			sfc_type != ARTIO_SFC_SLAB_Z ) {
		artio_fileset_destroy(handle);
		return NULL;
	}
	handle->sfc_type = sfc_type;

	if ( root_cells < 1 ) {
		artio_fileset_destroy(handle);
		return NULL;
	}

#ifdef ARTIO_MPI
	if ( MPI_Allreduce( &local_root_cells, &total_local_root_cells, 1,
				MPI_INT64_T, MPI_SUM, context->comm ) != MPI_SUCCESS ) {
		artio_fileset_destroy(handle);
		return NULL;
	}
#else
	total_local_root_cells = local_root_cells;
#endif /* ARTIO_MPI */

	if ( total_local_root_cells != root_cells ) {
		artio_fileset_destroy(handle);
		return NULL;
	}

	handle->num_local_root_cells = local_root_cells;
	handle->num_root_cells = root_cells;

	handle->nBitsPerDim = 0;
	tmp = handle->num_root_cells >> 3;
	while ( tmp ) {
		handle->nBitsPerDim++;
		tmp >>= 3;
	}
	handle->num_grid = 1<<handle->nBitsPerDim;

	if ( (int64_t)handle->num_grid*
			(int64_t)handle->num_grid*
			(int64_t)handle->num_grid != handle->num_root_cells ) {
		artio_fileset_destroy(handle);
		return NULL;
	}

	artio_parameter_set_int(handle, "sfc_type", handle->sfc_type);
	artio_parameter_set_long(handle, "num_root_cells", root_cells);

	artio_parameter_set_int(handle, "ARTIO_MAJOR_VERSION", ARTIO_MAJOR_VERSION );
	artio_parameter_set_int(handle, "ARTIO_MINOR_VERSION", ARTIO_MINOR_VERSION );

	return handle;
}

int artio_fileset_close(artio_fileset *handle) {
	char header_filename[256];
	artio_fh *head_fh;

	if ( handle == NULL ) {
		return ARTIO_ERR_INVALID_HANDLE;
	}

	if (handle->open_mode == ARTIO_FILESET_WRITE) {
		/* ensure we've flushed open particle and
		 * grid files before writing header */
		if ( handle->grid != NULL ) {
			artio_fileset_close_grid(handle);
		}

		if ( handle->particle != NULL ) {
			artio_fileset_close_particles(handle);
		}

		sprintf(header_filename, "%s.art", handle->file_prefix);
		head_fh = artio_file_fopen(header_filename,
				ARTIO_MODE_WRITE | ((handle->rank == 0) ? ARTIO_MODE_ACCESS : 0),
				handle->context);

		if (head_fh == NULL) {
			return ARTIO_ERR_FILE_CREATE;
		}

		if (0 == handle->rank) {
			artio_parameter_write(head_fh, handle->parameters );
		}

		artio_file_fclose(head_fh);
	}

	artio_fileset_destroy(handle);

	return ARTIO_SUCCESS;
}

artio_fileset *artio_fileset_allocate( char *file_prefix, int mode,
		const artio_context *context ) {
	int my_rank;
	int num_procs;

	artio_fileset *handle = (artio_fileset *)malloc(sizeof(artio_fileset));
	if ( handle != NULL ) {
		handle->parameters = artio_parameter_list_init();

#ifdef ARTIO_MPI
		handle->context = (artio_context *)malloc(sizeof(artio_context));
		if ( handle->context == NULL ) {
			return NULL;
		}
		memcpy( handle->context, context, sizeof(artio_context) );

		MPI_Comm_size(handle->context->comm, &num_procs);
		MPI_Comm_rank(handle->context->comm, &my_rank);
#else
		handle->context = NULL;

		num_procs = 1;
		my_rank = 0;
#endif /* MPI */

		strncpy(handle->file_prefix, file_prefix, 250);

		handle->open_mode = mode;
		handle->open_type = ARTIO_OPEN_HEADER;

		handle->rank = my_rank;
		handle->num_procs = num_procs;
		handle->endian_swap = 0;

		handle->num_grid = -1;
		handle->num_root_cells = -1;
		handle->num_local_root_cells = -1;

		handle->grid = NULL;
		handle->particle = NULL;
	}
	return handle;
}

void artio_fileset_destroy( artio_fileset *handle ) {
	if ( handle == NULL ) return;

	if ( handle->grid != NULL ) {
		artio_fileset_close_grid(handle);
	}

	if ( handle->particle != NULL ) {
		artio_fileset_close_particles(handle);
	}

	if ( handle->context != NULL ) free( handle->context );

	artio_parameter_list_free(handle->parameters);

	free(handle);
}

int artio_fileset_has_grid( artio_fileset *handle ) {
	int num_grid_files = 0;
	return ( handle->grid != NULL ||
			( artio_parameter_get_int( handle, "num_grid_files", &num_grid_files ) == ARTIO_SUCCESS &&
			  num_grid_files > 0 ) );
}

int artio_fileset_has_particles( artio_fileset *handle ) {
	int num_particle_files = 0;
	return ( handle->particle != NULL ||
			( artio_parameter_get_int( handle, "num_particle_files", &num_particle_files ) == ARTIO_SUCCESS &&
			  num_particle_files > 0 ) );
}

int artio_find_file( int64_t *file_sfc_index, int num_files, int64_t sfc) {
	int a, b, c;

	if ( sfc < file_sfc_index[0] || sfc >= file_sfc_index[num_files] ) {
		return -1;
	}

	a = 0;
	b = num_files;
	while ( a != b ) {
		c = ( a + b + 1) / 2;

		if ( sfc < file_sfc_index[c] ) {
			b = c-1;
		} else {
			a = c;
		}
	}
	return a;
}

int artio_fileset_distribute_sfc_to_files(
		artio_fileset *handle,
		int64_t *sfc_list,
		int64_t *sfc_size,
		int num_files,
		int allocation_strategy,
		const char file_suffix,
		int64_t *file_sfc_index,
		artio_fh **ffh ) {

	int proc, file, count;
	int mode, ret;
	int tag;
	char filename[1024];
	int64_t il, disp;
	int64_t offset, sfc_count;
	int64_t num_sfc_per_proc;
	int64_t sfc_offset_range_start;
	int64_t sfc_offset_range_end;
	int64_t sfc_offset_size;
	int64_t first_file_sfc;
	int64_t last_file_sfc;
	int64_t *sfc_offset_table;
	int64_t *sfc_list_recv;
	int64_t *sfc_size_recv;
	int64_t *sfc_list_send;
	int64_t *sfc_size_send;
	int64_t *send_sfc_count;
	int64_t *recv_sfc_count;
	int64_t *proc_sfc_offset;
	int *file_access;

#ifdef ARTIO_MPI
	int num_requests;
	MPI_Request *requests;
#endif /* ARTIO_MPI */

	/* distribute sfc equally amongst MPI ranks */
	num_sfc_per_proc = (handle->num_root_cells+handle->num_procs-1) / handle->num_procs;
	sfc_offset_range_start = handle->rank*num_sfc_per_proc;
	sfc_offset_range_end = MIN( handle->num_root_cells,
			(int64_t)(handle->rank+1)*num_sfc_per_proc );
	sfc_offset_size = sfc_offset_range_end - sfc_offset_range_start;

	for ( il = 0; il < handle->num_local_root_cells; il++ ) {
		if ( sfc_size[il] <= 0 ) {
			return ARTIO_ERR_INVALID_STATE;
		}
	}

#ifdef ARTIO_MPI
	/* communicate sfc lists and sizes to ensure a consistent order */
	sfc_list_recv = (int64_t *)malloc(sfc_offset_size*sizeof(int64_t));
	sfc_size_recv = (int64_t *)malloc(sfc_offset_size*sizeof(int64_t));
	sfc_list_send = (int64_t *)malloc(handle->num_local_root_cells*sizeof(int64_t));
	sfc_size_send = (int64_t *)malloc(handle->num_local_root_cells*sizeof(int64_t));
	send_sfc_count = (int64_t *)malloc(handle->num_procs*sizeof(int64_t));
	recv_sfc_count = (int64_t *)malloc(handle->num_procs*sizeof(int64_t));
	proc_sfc_offset = (int64_t *)malloc(handle->num_procs*sizeof(int64_t));
	if ( sfc_list_recv == NULL || sfc_size_recv == NULL ||
			sfc_list_send == NULL || sfc_size_send == NULL ||
			send_sfc_count == NULL || recv_sfc_count == NULL ||
			proc_sfc_offset == NULL ) {
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	for ( il = 0; il < sfc_offset_size; il++ ) {
		sfc_size_recv[il] = -101;
	}

	/* count the number of sfcs to send to each process */
	for ( proc = 0; proc < handle->num_procs; proc++ ) {
		send_sfc_count[proc] = 0;
	}
	for ( il = 0; il < handle->num_local_root_cells; il++ ) {
		proc = sfc_list[il] / num_sfc_per_proc;
		send_sfc_count[proc]++;
	}

	/* let each rank know how many to expect */
	MPI_Alltoall( send_sfc_count, 1, MPI_INT64_T,
			recv_sfc_count, 1, MPI_INT64_T,
			handle->context->comm );

	/* ensure we've received the correct number of sfc indices */
	sfc_count = 0;
	for ( proc = 0; proc < handle->num_procs; proc++ ) {
		sfc_count += recv_sfc_count[proc];
	}

	if ( sfc_count != sfc_offset_size ) {
		return ARTIO_ERR_INVALID_STATE;
	}

	/* count the number of send and receive requests to allocate */
	num_requests = 0;
	for ( proc = 0; proc < handle->num_procs; proc++ ) {
		if ( recv_sfc_count[proc] > 0 ) {
			num_requests += 2*(recv_sfc_count[proc] + ARTIO_IO_MAX - 1) / ARTIO_IO_MAX;
		}
		if ( send_sfc_count[proc] > 0 ) {
			num_requests += 2*(send_sfc_count[proc] + ARTIO_IO_MAX - 1) / ARTIO_IO_MAX;
		}
	}

	requests = (MPI_Request *)malloc( num_requests*sizeof(MPI_Request) );
	if ( requests == NULL ) {
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}
	num_requests = 0;

	disp = 0;
	for ( proc = 0; proc < handle->num_procs; proc++ ) {
		tag = 0;
		/* internal loop handles sending 32-bit safe ARTIO_IO_MAX at a time */
		for ( sfc_count = 0; sfc_count < recv_sfc_count[proc]; sfc_count += ARTIO_IO_MAX ) {
			count = MIN( recv_sfc_count[proc]-sfc_count, ARTIO_IO_MAX );
			MPI_Irecv( &sfc_list_recv[disp], count, MPI_INT64_T,
					proc, 3*tag, handle->context->comm, &requests[num_requests++] );
			MPI_Irecv( &sfc_size_recv[disp], count, MPI_INT64_T,
					proc, 3*tag+1, handle->context->comm, &requests[num_requests++] );
			disp += count;
			tag++;
		}
	}

	proc_sfc_offset[0] = 0;
	for ( proc = 1; proc < handle->num_procs; proc++ ) {
		proc_sfc_offset[proc] = proc_sfc_offset[proc-1] + send_sfc_count[proc-1];
	}

	/* copy sfc into per-proc arrays */
	for ( il = 0; il < handle->num_local_root_cells; il++ ) {
		proc = sfc_list[il] / num_sfc_per_proc;
		sfc_list_send[proc_sfc_offset[proc]] = sfc_list[il];
		sfc_size_send[proc_sfc_offset[proc]] = sfc_size[il];
		proc_sfc_offset[proc]++;
	}

	disp = 0;
	for ( proc = 0; proc < handle->num_procs; proc++ ) {
		tag = 0;
		for ( sfc_count = 0; sfc_count < send_sfc_count[proc]; sfc_count += ARTIO_IO_MAX ) {
			count = MIN( send_sfc_count[proc]-sfc_count, ARTIO_IO_MAX );
			MPI_Isend( &sfc_list_send[disp], count,
					MPI_INT64_T, proc, 3*tag, handle->context->comm,
					&requests[num_requests++] );
			MPI_Isend( &sfc_size_send[disp], count,
					MPI_INT64_T, proc, 3*tag+1, handle->context->comm,
					&requests[num_requests++] );
			tag++;
			disp += count;
		}
	}

	MPI_Waitall( num_requests, requests, MPI_STATUS_IGNORE );
#else
	/* if only 1 rank then we have the complete list */
	sfc_list_recv = sfc_list;
	sfc_size_recv = sfc_size;
#endif /* ARTIO_MPI */

	/* compute the cumulative size index */
	sfc_offset_table = (int64_t *)malloc( sfc_offset_size*sizeof(int64_t) );
	if ( sfc_offset_table == NULL ) {
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

	/* copy sizes into sfc order */
	for ( il = 0; il < sfc_offset_size; il++ ) {
		sfc_offset_table[ sfc_list_recv[il] - sfc_offset_range_start ] = sfc_size_recv[il];
	}

	/* compute running size */
#ifdef ARTIO_MPI
	if ( handle->rank > 0 ) {
		MPI_Recv( &offset, 1, MPI_INT64_T, handle->rank-1,
				handle->rank-1, handle->context->comm, MPI_STATUS_IGNORE );
	} else {
		offset = 0;
	}
#else
	offset = 0;
#endif /* ARTIO_MPI */

	for ( il = 0; il < sfc_offset_size; il++ ) {
		disp = sfc_offset_table[il];
		sfc_offset_table[il] = offset;
		offset += disp;
	}

#ifdef ARTIO_MPI
	if ( handle->rank < handle->num_procs-1 ) {
		MPI_Send( &offset, 1, MPI_INT64_T, handle->rank+1,
				handle->rank, handle->context->comm );
	}
#endif /* ARTIO_MPI */

	/* use allocation strategy to divide sfc range into files */
	switch (allocation_strategy) {
		case ARTIO_ALLOC_EQUAL_SFC:
			if ( num_files > handle->num_root_cells ) {
				return ARTIO_ERR_INVALID_FILE_NUMBER;
			}

			for (file = 0; file < num_files; file++) {
				file_sfc_index[file] =
					(handle->num_root_cells*file+num_files-1) / num_files;
			}
			file_sfc_index[num_files] = handle->num_root_cells;
			break;
		case ARTIO_ALLOC_EQUAL_SIZE: /* not implemented yet */
		case ARTIO_ALLOC_EQUAL_PROC: /* deprecated */
		default:
			return ARTIO_ERR_INVALID_ALLOC_STRATEGY;
	}

	/* convert size arrays into offset tables */
#ifdef ARTIO_MPI
	if ( handle->rank == 0 ) {
#endif
		/* negative to add space for file offset header */
		offset = sfc_offset_table[0] - (file_sfc_index[1]-file_sfc_index[0])*sizeof(int64_t);
		file = 0;
#ifdef ARTIO_MPI
	} else {
		file = artio_find_file(file_sfc_index, num_files, sfc_offset_range_start - 1);
		MPI_Recv( &offset, 1, MPI_INT64_T, handle->rank-1,
			handle->rank-1, handle->context->comm, MPI_STATUS_IGNORE );
	}
#endif

	for ( il = 0; il < sfc_offset_size; il++ ) {
		if ( sfc_offset_range_start+il == file_sfc_index[file+1] ) {
			file++;
			offset = sfc_offset_table[il] - (file_sfc_index[file+1]-file_sfc_index[file])*sizeof(int64_t);
		}
		sfc_offset_table[il] -= offset;
	}

#ifdef ARTIO_MPI
	if ( handle->rank < handle->num_procs-1 ) {
		MPI_Send( &offset, 1, MPI_INT64_T, handle->rank+1,
				handle->rank, handle->context->comm );
	}
#endif /* ARTIO_MPI */

	/* put local offset table into sfc_size */
#ifdef ARTIO_MPI
	/* set up recives */
	num_requests = 0;
	disp = 0;
	for ( proc = 0; proc < handle->num_procs; proc++ ) {
		tag = 0;
		for ( sfc_count = 0; sfc_count < send_sfc_count[proc]; sfc_count += ARTIO_IO_MAX ) {
			count = MIN( send_sfc_count[proc]-sfc_count, ARTIO_IO_MAX );
			MPI_Irecv( &sfc_size_send[disp], count, MPI_INT64_T,
					proc, 3*tag+2, handle->context->comm, &requests[num_requests++] );
			disp += count;
			tag++;
		}
	}

	/* reorder offset table to proc order */
	for ( il = 0; il < sfc_offset_size; il++ ) {
		sfc_size_recv[il] = sfc_offset_table[ sfc_list_recv[il] - sfc_offset_range_start ];
	}

	/* set up sends */
	disp = 0;
	for ( proc = 0; proc < handle->num_procs; proc++ ) {
		tag = 0;
		for ( sfc_count = 0; sfc_count < recv_sfc_count[proc]; sfc_count += ARTIO_IO_MAX ) {
			count = MIN( recv_sfc_count[proc]-sfc_count, ARTIO_IO_MAX );
			MPI_Isend( &sfc_size_recv[disp], count,
					MPI_INT64_T, proc, 3*tag+2, handle->context->comm,
					&requests[num_requests++] );
			disp += count;
			tag++;
		}
	}

	MPI_Waitall( num_requests, requests, MPI_STATUS_IGNORE );

	proc_sfc_offset[0] = 0;
	for ( proc = 1; proc < handle->num_procs; proc++ ) {
		proc_sfc_offset[proc] = proc_sfc_offset[proc-1] + send_sfc_count[proc-1];
	}

	/* copy sfc into per-proc arrays */
	for ( il = 0; il < handle->num_local_root_cells; il++ ) {
		proc = sfc_list[il] / num_sfc_per_proc;
		sfc_size[il] = sfc_size_send[ proc_sfc_offset[proc] ];
		proc_sfc_offset[proc]++;
	}

	free( sfc_list_recv );
	free( sfc_size_send );
	free( sfc_size_recv );
	free( sfc_list_send );
	free( send_sfc_count );
	free( recv_sfc_count );
	free( proc_sfc_offset );
#else
	for ( il = 0; il < handle->num_local_root_cells; il++ ) {
		sfc_size[il] = sfc_offset_table[ sfc_list[il] ];
	}
#endif /* ARTIO_MPI */

	/* determine which files we write to */
	file_access = (int *)malloc(num_files * sizeof(int));
	if ( file_access == NULL ) {
		return ARTIO_ERR_MEMORY_ALLOCATION;
	}

#ifdef ARTIO_MPI
	for ( file = 0; file < num_files; file++ ) {
		if ( file_sfc_index[file] < sfc_offset_range_end &&
				file_sfc_index[file+1] > sfc_offset_range_start ) {
			file_access[file] = 1;
		} else {
			file_access[file] = 0;
		}
	}

	/* also access files where we have local sfc */
	for ( il = 0; il < handle->num_local_root_cells; il++ ) {
		file = artio_find_file(file_sfc_index, num_files, sfc_list[il]);
		file_access[file] = 1;
	}
#else
	for ( file = 0; file < num_files; file++ ) {
		file_access[file] = 1;
	}
#endif /* ARTIO_MPI */

	/* open file handles and write offset tables */
	for (file = 0; file < num_files; file++) {
		sprintf(filename, "%s.%c%03d", handle->file_prefix,
			file_suffix, file);

		mode = ARTIO_MODE_WRITE;
		if ( file_access[file] ) {
			mode |= ARTIO_MODE_ACCESS;
		}

		ffh[file] = artio_file_fopen(filename, mode, handle->context);
		if ( ffh[file] == NULL ) {
			return ARTIO_ERR_FILE_CREATE;
		}

		/* write sfc offset header if we hold the offset table */
		/* NOTE: bug is off-by-one in sfc_offset_range_end vs file index */
		if ( file_sfc_index[file] < sfc_offset_range_end &&
				file_sfc_index[file+1] > sfc_offset_range_start ) {

			first_file_sfc = MAX( sfc_offset_range_start,
					file_sfc_index[file] );
			last_file_sfc = MIN( sfc_offset_range_end-1,
					file_sfc_index[file+1]-1 );

			/* seek and write our portion of sfc table */
			ret = artio_file_fseek(ffh[file],
					(first_file_sfc - file_sfc_index[file])*sizeof(int64_t),
					ARTIO_SEEK_SET);
			if ( ret != ARTIO_SUCCESS ) {
				return ret;
			}

			ret = artio_file_fwrite(ffh[file],
					&sfc_offset_table[first_file_sfc - sfc_offset_range_start],
					last_file_sfc - first_file_sfc + 1, ARTIO_TYPE_LONG);
			if ( ret != ARTIO_SUCCESS ) {
				return ret;
			}
		}
	}

	free( sfc_offset_table );
	free( file_access );

	return ARTIO_SUCCESS;
}
