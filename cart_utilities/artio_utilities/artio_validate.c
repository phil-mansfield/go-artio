#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>

#include "artio.h"

int ret;
#define CHECK_STATUS(f)		\
	ret = f; \
	if ( ret != ARTIO_SUCCESS ) { \
		fprintf(stderr, "artio failure code %d at %s:%d\n", ret, __FILE__, __LINE__ ); \
		exit(1); \
	} 

int main( int argc, char *argv[] ) {
	int i, j;
	int level;
	int ioct;
	int64_t num_root_cells, sfc;
	int num_levels, num_tree_levels;
	int *num_octs_per_level;
	int next_level_count;
	int num_vars;
	int refined[8];
	float *variables;
	float *min_var, *max_var;
	char **label;

	if ( argc != 2 ) {
		fprintf(stderr,"Usage: %s fileset_prefix\n",argv[0]);
		exit(1);
	}

	artio_fileset *handle = artio_fileset_open( argv[1], 0, NULL );
	if ( handle == NULL ) {
		fprintf(stderr,"Unable to open fileset %s\n", argv[1] );
		exit(1);
	}

	if ( artio_fileset_has_grid(handle) ) {
		CHECK_STATUS( artio_fileset_open_grid(handle) );
	
		/* determine number of variables */	
		CHECK_STATUS( artio_parameter_get_int(handle, "num_grid_variables", &num_vars) );

		label = (char **)malloc( num_vars*sizeof(char *));
		for ( i = 0; i < num_vars; i++ ) {
			label[i] = (char *)malloc( ARTIO_MAX_STRING_LENGTH*sizeof(char) );
		}

		CHECK_STATUS( artio_parameter_get_string_array(handle, "grid_variable_labels", num_vars, label) );

		variables = (float *)malloc( 8*num_vars*sizeof(float) );
		min_var = (float *)malloc( num_vars*sizeof(float) );
		max_var = (float *)malloc( num_vars*sizeof(float) );
		for ( i = 0; i < num_vars; i++ ) {
			min_var[i] = 1e30;
			max_var[i] = -1e30;
		}

		/* get number of root cells */
		CHECK_STATUS( artio_parameter_get_long(handle, "num_root_cells", &num_root_cells) );

		/* get number of refinement levels */
		CHECK_STATUS( artio_parameter_get_int(handle, "max_refinement_level", &num_levels) );

		num_octs_per_level = malloc( num_levels*sizeof(int) );
	
		CHECK_STATUS( artio_grid_cache_sfc_range(handle, 0, num_root_cells-1) );
	
		/* loop through all sfc indices */
		for ( sfc = 0; sfc < num_root_cells; sfc++ ) {
			CHECK_STATUS( artio_grid_read_root_cell_begin(handle, sfc, NULL, variables, &num_tree_levels, num_octs_per_level ) );
			assert( num_tree_levels >= 0 && num_tree_levels <= num_levels );
			assert( num_tree_levels == 0 || num_octs_per_level[0] == 1 );
			for ( level = 1; level < num_tree_levels-1; level++ ) {
				assert( num_octs_per_level[level] >= 0 && num_octs_per_level[level] <= 8*num_octs_per_level[level-1] );
			}

			for ( i = 0; i < num_vars; i++ ) {
				min_var[i] = ( variables[i] < min_var[i] ) ? variables[i] : min_var[i];
				max_var[i] = ( variables[i] > max_var[i] ) ? variables[i] : max_var[i];
			}

			for ( level = 1; level <= num_tree_levels; level++ ) {
				CHECK_STATUS( artio_grid_read_level_begin(handle, level) );

				next_level_count = 0;
				for ( ioct = 0; ioct < num_octs_per_level[level-1]; ioct++ ) {
					CHECK_STATUS( artio_grid_read_oct(handle, NULL, variables, refined) );

					for ( i = 0; i < 8; i++ ) {
						for ( j = 0; j < num_vars; j++ ) {
							min_var[j] = ( variables[num_vars*i+j] < min_var[j] ) ? variables[num_vars*i+j] : min_var[j];
							max_var[j] = ( variables[num_vars*i+j] > max_var[j] ) ? variables[num_vars*i+j] : max_var[j];
						}

						if ( refined[i] ) {
							next_level_count++;
						}
					}
				}

				assert( (level < num_tree_levels && next_level_count == num_octs_per_level[level]) ||
					(level == num_tree_levels && next_level_count == 0) );

				CHECK_STATUS( artio_grid_read_level_end(handle) );
			}

			CHECK_STATUS( artio_grid_read_root_cell_end(handle) );
		}

		for ( i = 0; i < num_vars; i++ ) {
			printf("%32s: %14.6e %14.6e\n", label[i], min_var[i], max_var[i] );
		}

		CHECK_STATUS( artio_grid_clear_sfc_cache(handle) );
	}

	CHECK_STATUS( artio_fileset_close(handle) );
}
