#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <limits.h>
#include <assert.h>
#include <string.h>

#include "artio.h"

int ret;
#define CHECK_STATUS(f)		\
	ret = f; \
	if ( ret != ARTIO_SUCCESS ) { \
		fprintf(stderr, "artio failure code %d at %s:%d\n", ret, __FILE__, __LINE__ ); \
		exit(1); \
	}

#define allocate(type, size) (type *)allocate_worker((size)*sizeof(type),__FILE__,__LINE__)
void* allocate_worker(size_t size, const char *file, int line) {
	void *ptr = NULL;

	if(size > 0) {
		ptr = malloc(size);
		if(ptr == NULL) {
			fprintf(stderr, "Failure allocating %ld bytes in file %s, line %d", size, file, line );
			exit(1);
		}
		memset(ptr, 0, size);
	}
	return ptr;
}


void progress(const char *stage, int64_t num_root_cells, int64_t sfc)
{
  if(sfc%1000 == 0)
    {
      printf("%s, sfc = %ld (%ld)\r",stage,sfc,num_root_cells);
      fflush(stdout);
    }
}


int main( int argc, char *argv[] ) {
	int i, j;
	int num_new_files;
	float jf, f;
	char key[ARTIO_MAX_STRING_LENGTH];
	int type, length;
	int level;
	int ioct;
	int total_octs;
	int64_t num_root_cells, sfc;
	int num_levels;
	int *num_octs_per_level;
	int *num_octs_per_sfc;
	int *num_levels_per_sfc;
	int num_vars;
	int refined[8];
	float *variables;
	char **label;
	int64_t *grid_file_sfc_index;
	int64_t *new_grid_file_sfc_index;
	int64_t *particle_file_sfc_index;
	int64_t *new_particle_file_sfc_index;

	int num_particle_files;
	int num_grid_files;

	int64_t pid;
	int num_species;
	int subspecies;
	int *num_particles_per_species;
	int *num_primary_variables;
	int *num_secondary_variables;
	int *num_particles_per_species_per_root_tree;
	char ***primary_variable_labels_per_species;
	char ***secondary_variable_labels_per_species;
	char species_label[ARTIO_MAX_STRING_LENGTH];

	double *primary_variables;
	float *secondary_variables;

	if ( argc != 4 ) {
		fprintf(stderr,"Usage: %s input_prefix output_prefix num_new_files\n",argv[0]);
		exit(1);
	}

	num_new_files = atoi(argv[3]);

	artio_fileset *handle = artio_fileset_open( argv[1], 0, NULL );
	if ( handle == NULL ) {
		fprintf(stderr,"Unable to open fileset %s\n", argv[1] );
		exit(1);
	}

	CHECK_STATUS( artio_parameter_get_long(handle, "num_root_cells", &num_root_cells) );

	artio_fileset *output = artio_fileset_create( argv[2], 0,
		num_root_cells, 0, num_root_cells-1, NULL );
	if ( output == NULL ) {
		fprintf(stderr, "Error creating artio fileset %s\n", argv[2]);
		exit(1);
	}

	if ( artio_fileset_has_grid(handle) ) {
		CHECK_STATUS( artio_fileset_open_grid(handle) );

		CHECK_STATUS( artio_parameter_get_int(handle, "num_grid_files", &num_grid_files) );
		printf("num_grid_files = %d\n", num_grid_files );

		grid_file_sfc_index = allocate(int64_t, num_grid_files+1);
		new_grid_file_sfc_index = allocate(int64_t, num_new_files+1);
		CHECK_STATUS( artio_parameter_get_long_array(handle, "grid_file_sfc_index",
				num_grid_files+1, grid_file_sfc_index) );
		for ( i = 0; i <= num_grid_files; i++ ) {
			printf("grid_file_sfc_index[%d] = %ld\n", i, grid_file_sfc_index[i]);
		}

		for ( i = 0; i < num_new_files; i++ ) {
			jf = (float)i*(float)num_grid_files/(float)num_new_files;
			j = (int)jf;
			f = jf - floor(jf);
			new_grid_file_sfc_index[i] = grid_file_sfc_index[j] +
				(int64_t)(f*(grid_file_sfc_index[j+1] - grid_file_sfc_index[j]));
		}
		new_grid_file_sfc_index[num_new_files] = num_root_cells;

		free(grid_file_sfc_index);

		for ( i = 0; i <= num_new_files; i++ ) {
			printf("new_grid_file_sfc_index[%d] = %ld\n", i, new_grid_file_sfc_index[i]);
		}

		assert( new_grid_file_sfc_index[0] == 0 );
		for ( i = 1; i < num_new_files; i++ ) {
			assert( new_grid_file_sfc_index[i] > new_grid_file_sfc_index[i-1] );
		}
		assert( new_grid_file_sfc_index[num_new_files] == num_root_cells );

		/* set new allocation */
		CHECK_STATUS( artio_parameter_set_long_array(output, "grid_file_sfc_index",
			num_new_files+1, new_grid_file_sfc_index) );

#ifdef ARTIO_REMAP_POSIX
		/* in this case the number of files equals the number of processes, need
		 * to override the domain decomposition */
		CHECK_STATUS( artio_parameter_set_long_array(output, "mpi_task_sfc_index",
					num_new_files+1, new_grid_file_sfc_index) );
#endif /* ARTIO_REMAP_POSIX */

		/* determine number of variables */
		CHECK_STATUS( artio_parameter_get_int(handle, "num_grid_variables", &num_vars) );

		printf("num_variables = %d\n", num_vars);

		/* read grid labels */
		label = allocate(char *, num_vars);
		for ( i = 0; i < num_vars; i++ ) {
			label[i] = allocate(char, ARTIO_MAX_STRING_LENGTH);
		}

		CHECK_STATUS( artio_parameter_get_string_array(handle, "grid_variable_labels", num_vars, label) );

		for ( i = 0; i < num_vars; i++ ) {
			printf("variable[%d] = %s\n", i, label[i]);
		}

		/* get number of refinement levels */
		CHECK_STATUS( artio_parameter_get_int(handle, "max_refinement_level", &num_levels) );

		printf("num_levels = %d\n", num_levels);

		num_octs_per_level = allocate(int, num_levels);
		num_octs_per_sfc = allocate(int, num_root_cells);
		num_levels_per_sfc = allocate(int, num_root_cells);

		CHECK_STATUS( artio_grid_cache_sfc_range(handle, 0, num_root_cells-1) );

		/* loop through all sfc indices to collect refinement information */
		for ( sfc = 0; sfc < num_root_cells; sfc++ ) {

			progress("preparing",num_root_cells,sfc);

			CHECK_STATUS( artio_grid_read_root_cell_begin(handle, sfc, NULL, NULL,
				&num_levels_per_sfc[sfc], num_octs_per_level ) );
			num_octs_per_sfc[sfc] = 0;
			for ( level = 0; level < num_levels_per_sfc[sfc]; level++ ) {
				num_octs_per_sfc[sfc] += num_octs_per_level[level];
			}
			CHECK_STATUS( artio_grid_read_root_cell_end(handle) );
		}

		CHECK_STATUS( artio_fileset_add_grid( output,
			num_new_files, ARTIO_ALLOC_MANUAL,
			num_vars, label, num_levels_per_sfc,
			num_octs_per_sfc ) );

		for ( i = 0; i < num_vars; i++ ) {
			free(label[i]);
		}
		free(label);

		variables = allocate(float, 8*num_vars);

		for ( sfc = 0; sfc < num_root_cells; sfc++ ) {

			progress("remapping grid",num_root_cells,sfc);

			CHECK_STATUS( artio_grid_read_root_cell_begin(handle, sfc, NULL, variables, &num_levels, num_octs_per_level ) );

			if ( num_levels != num_levels_per_sfc[sfc] ) {
				fprintf(stderr, "Error: number of levels does not match for sfc %ld, %d vs %d\n", sfc, num_levels, num_levels_per_sfc[sfc]);
				exit(1);
			}

			total_octs = 0;
			for ( level = 0; level < num_levels; level++ ) {
				total_octs += num_octs_per_level[level];
			}

			if ( total_octs != num_octs_per_sfc[sfc] ) {
				fprintf(stderr, "Error: number of octs does not match for sfc %ld, %d vs %d\n", sfc, total_octs, num_octs_per_sfc[sfc]);
				for ( level = 0; level < num_levels; level++ ) {
					fprintf(stderr, "num_octs_per_level[%d] = %d\n", level+1, num_octs_per_level[level]);
				}
				exit(1);
			}

			CHECK_STATUS( artio_grid_write_root_cell_begin(output, sfc, variables, num_levels, num_octs_per_level) );

			for ( level = 1; level <= num_levels; level++ ) {
				CHECK_STATUS( artio_grid_read_level_begin(handle, level) );
				CHECK_STATUS( artio_grid_write_level_begin(output, level) );

				for ( ioct = 0; ioct < num_octs_per_level[level-1]; ioct++ ) {
					CHECK_STATUS( artio_grid_read_oct(handle, NULL, variables, refined) );
					CHECK_STATUS( artio_grid_write_oct(output, variables, refined) );
				}

				CHECK_STATUS( artio_grid_read_level_end(handle) );
				CHECK_STATUS( artio_grid_write_level_end(output) );
			}

			CHECK_STATUS( artio_grid_read_root_cell_end(handle) );
			CHECK_STATUS( artio_grid_write_root_cell_end(output) );
		}

		free(variables);
		free(num_levels_per_sfc);
		free(num_octs_per_sfc);
		free(num_octs_per_level);

		CHECK_STATUS( artio_fileset_close_grid(handle) );
		CHECK_STATUS( artio_fileset_close_grid(output) );

		printf("\ndone writing grid data\n");
	}

	if ( artio_fileset_has_particles(handle) ) {
		CHECK_STATUS( artio_fileset_open_particles(handle) );

		CHECK_STATUS( artio_parameter_get_int(handle, "num_particle_files", &num_particle_files) );

		printf("num_particles_files = %d\n", num_particle_files);

#ifdef ARTIO_REMAP_POSIX
		if ( artio_fileset_has_grid(handle) ) {
			new_particle_file_sfc_index = new_grid_file_sfc_index;
		} else {
#endif
		particle_file_sfc_index = allocate(int64_t, num_particle_files+1);
		new_particle_file_sfc_index = allocate(int64_t, num_new_files+1);
		CHECK_STATUS( artio_parameter_get_long_array(handle, "particle_file_sfc_index",
					num_particle_files+1, particle_file_sfc_index) );
		for ( i = 0; i <= num_particle_files; i++ ) {
			printf("particle_file_sfc_index[%d] = %ld\n", i, particle_file_sfc_index[i]);
		}

		for ( i = 0; i < num_new_files; i++ ) {
			jf = (float)i*(float)num_particle_files/(float)num_new_files;
			j = (int)jf;
			f = jf - floor(jf);
			new_particle_file_sfc_index[i] = particle_file_sfc_index[j] +
				(int64_t)(f*(particle_file_sfc_index[j+1] - particle_file_sfc_index[j]));
		}
		new_particle_file_sfc_index[num_new_files] = num_root_cells;

		free(particle_file_sfc_index);

		for ( i = 0; i <= num_new_files; i++ ) {
			printf("new_particle_file_sfc_index[%d] = %ld\n", i, new_particle_file_sfc_index[i]);
		}

		assert( new_particle_file_sfc_index[0] == 0 );
		for ( i = 1; i < num_new_files; i++ ) {
			assert( new_particle_file_sfc_index[i] > new_particle_file_sfc_index[i-1] );
		}
		assert( new_particle_file_sfc_index[num_new_files] == num_root_cells );

#ifdef ARTIO_REMAP_POSIX
		/* in this case the number of files equals the number of processes, need
		 * to override the domain decomposition */
		CHECK_STATUS( artio_parameter_set_long_array(output, "mpi_task_sfc_index",
					num_new_files+1, new_particle_file_sfc_index) );
		} /* artio_fileset has grid */
#endif /* ARTIO_REMAP_POSIX */

		/* set new allocation */
		CHECK_STATUS( artio_parameter_set_long_array(output, "particle_file_sfc_index",
					num_new_files+1, new_particle_file_sfc_index) );

		free( new_particle_file_sfc_index );

		/* load particle parameters */
		CHECK_STATUS( artio_parameter_get_int(handle, "num_particle_species", &num_species) );

		printf("num_particle_species = %d\n", num_species);

		label = allocate(char *, num_species);
		for ( i = 0; i < num_species; i++ ) {
			label[i] = allocate(char, ARTIO_MAX_STRING_LENGTH);
		}

		CHECK_STATUS( artio_parameter_get_string_array(handle, "particle_species_labels",
				num_species, label) );

		num_primary_variables = allocate(int, num_species);
		num_secondary_variables = allocate(int, num_species);

		CHECK_STATUS( artio_parameter_get_int_array(handle, "num_primary_variables",
				num_species, num_primary_variables) );
		CHECK_STATUS( artio_parameter_get_int_array(handle, "num_secondary_variables",
				num_species, num_secondary_variables) );

		primary_variable_labels_per_species = allocate(char **, num_species);
		secondary_variable_labels_per_species = allocate(char **, num_species);

		for(i=0;i<num_species;i++) {
			printf("species %d: %s\n", i, label[i]);

			primary_variable_labels_per_species[i] = allocate(char *, num_primary_variables[i]);
			for (j = 0; j < num_primary_variables[i]; j++) {
				primary_variable_labels_per_species[i][j] = allocate(char, ARTIO_MAX_STRING_LENGTH);
			}

			sprintf( species_label, "species_%02u_primary_variable_labels", i );
			CHECK_STATUS( artio_parameter_get_string_array( handle, species_label,
					num_primary_variables[i], primary_variable_labels_per_species[i] ) );

			printf("primary variables:");
			for ( j = 0; j < num_primary_variables[i]; j++ ) {
				printf(" %s", primary_variable_labels_per_species[i][j]);
			}
			printf("\n");

			if ( num_secondary_variables[i] > 0 ) {
				secondary_variable_labels_per_species[i] = allocate(char *, num_secondary_variables[i]);
				for (j = 0; j < num_secondary_variables[i]; j++) {
					secondary_variable_labels_per_species[i][j] = allocate(char, ARTIO_MAX_STRING_LENGTH);
				}

				sprintf( species_label, "species_%02u_secondary_variable_labels", i );
				CHECK_STATUS( artio_parameter_get_string_array( handle, species_label,
						num_secondary_variables[i], secondary_variable_labels_per_species[i] ) );

				printf("secondary variables:");
				for ( j = 0; j < num_secondary_variables[i]; j++ ) {
					printf(" %s", secondary_variable_labels_per_species[i][j]);
				}
				printf("\n");
			}
		}

		num_particles_per_species_per_root_tree = allocate(int, num_species*num_root_cells);

		CHECK_STATUS( artio_particle_cache_sfc_range(handle, 0, num_root_cells-1) );

		for ( sfc = 0; sfc < num_root_cells; sfc++ ) {
			CHECK_STATUS( artio_particle_read_root_cell_begin(handle, sfc,
				&num_particles_per_species_per_root_tree[num_species*sfc]) );
			CHECK_STATUS( artio_particle_read_root_cell_end(handle) );
		}

		CHECK_STATUS( artio_fileset_add_particles(output,
						num_new_files, ARTIO_ALLOC_MANUAL,
						num_species, label,
						num_primary_variables,
						num_secondary_variables,
						primary_variable_labels_per_species,
						secondary_variable_labels_per_species,
						num_particles_per_species_per_root_tree ) );

		for ( i = 0; i < num_species; i++ ) {
			free(label[i]);

			for ( j = 0; j < num_primary_variables[i]; j++ ) {
				free(primary_variable_labels_per_species[i][j]);
			}
			free(primary_variable_labels_per_species[i]);
			if ( num_secondary_variables[i] > 0 ) {
				for ( j = 0; j < num_secondary_variables[i]; j++ ) {
					free(secondary_variable_labels_per_species[i][j]);
				}
				free(secondary_variable_labels_per_species[i]);
			}
		}
		free(primary_variable_labels_per_species);
		free(secondary_variable_labels_per_species);
		free(label);

#ifndef DEBUG
		free(num_particles_per_species_per_root_tree);
#endif

		num_particles_per_species = allocate(int, num_species);

		num_vars = 1;
		for ( i = 0; i < num_species; i++ ) {
			if ( num_primary_variables[i] > num_vars ) {
				num_vars = num_primary_variables[i];
			}
		}	
		primary_variables = allocate(double, num_vars);

		num_vars = 1;
		for ( i = 0; i < num_species; i++ ) {
			if ( num_secondary_variables[i] > num_vars ) {
				num_vars = num_secondary_variables[i];
			}
		}
		secondary_variables = allocate(float, num_vars);

		for ( sfc = 0; sfc < num_root_cells; sfc++ ) {

			progress("remapping particles",num_root_cells,sfc);

			CHECK_STATUS( artio_particle_read_root_cell_begin(handle, sfc,
						num_particles_per_species) );

#ifdef DEBUG
			for ( i = 0; i < num_species; i++ ) {
				if ( num_particles_per_species[i] != num_particles_per_species_per_root_tree[num_species*sfc+i] ) {
					fprintf(stderr, "Error: number of particles for species %d does not match for sfc %ld, %d vs %d\n",
						i, sfc, num_particles_per_species[i], num_particles_per_species_per_root_tree[num_species*sfc+i]);
					exit(1);
				}
			}
#endif

			CHECK_STATUS( artio_particle_write_root_cell_begin(output, sfc,
				num_particles_per_species) );

			for ( i = 0; i < num_species; i++ ) {
				CHECK_STATUS( artio_particle_read_species_begin(handle, i) );
				CHECK_STATUS( artio_particle_write_species_begin(output, i) );

				for ( j = 0; j < num_particles_per_species[i]; j++ ) {
					CHECK_STATUS( artio_particle_read_particle(handle, &pid, &subspecies,
							primary_variables, secondary_variables) );
					CHECK_STATUS( artio_particle_write_particle(output, pid, subspecies,
							primary_variables, secondary_variables) );
				}

				CHECK_STATUS( artio_particle_read_species_end(handle) );
				CHECK_STATUS( artio_particle_write_species_end(output) );
			}

			CHECK_STATUS( artio_particle_read_root_cell_end(handle) );
			CHECK_STATUS( artio_particle_write_root_cell_end(output) );
		}

		free(num_primary_variables);
		free(num_secondary_variables);
		free(primary_variables);
		free(secondary_variables);
		free(num_particles_per_species);

#ifdef DEBUG
		free(num_particles_per_species_per_root_tree);
#endif

		CHECK_STATUS( artio_fileset_close_particles(handle) );
		CHECK_STATUS( artio_fileset_close_particles(output) );

		printf("\ndone writing particle data\n");
	}

	/* copy any parameters in handle that are not in output */
	while (artio_parameter_iterate(handle, key, &type, &length) == ARTIO_SUCCESS) {
		if ( !artio_parameter_has_key(output, key) ) {
			artio_parameter_copy(handle, output, key);
		}
	}

	CHECK_STATUS( artio_fileset_close(handle) );
	CHECK_STATUS( artio_fileset_close(output) );

	return 0;
}
