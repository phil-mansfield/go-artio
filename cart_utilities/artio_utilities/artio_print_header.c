#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

#include "artio.h"

int main( int argc, char *argv[] ) {
	int i;
	int type, length;
	char key[64];
	int *tmp_int;
	char **tmp_string;
	int64_t *tmp_long;
	double *tmp_double;
	float *tmp_float;

	if ( argc != 2 ) {
		fprintf(stderr,"Usage: %s fileset_prefix\n",argv[0]);
		exit(1);
	}

	artio_fileset *handle = artio_fileset_open( argv[1], 0, NULL );
	if ( handle == NULL ) {
		fprintf(stderr,"Unable to open fileset %s\n", argv[1] );
		exit(1);
	}

	while ( artio_parameter_iterate( handle, key, &type, &length ) == ARTIO_SUCCESS ) {
		switch (type) {
			case ARTIO_TYPE_STRING :
				tmp_string = (char **)malloc( length*sizeof(char *) );
				for ( i = 0; i < length; i++ ) {
					tmp_string[i] = (char *)malloc( ARTIO_MAX_STRING_LENGTH*sizeof(char) );
				}
				artio_parameter_get_string_array(handle, key, length, tmp_string );
				printf("%36s | %6s |", key, "STRING"); 
				for ( i = 0; i < length; i++ ) {
					printf(" '%s'", tmp_string[i] );
					free( tmp_string[i] );
				}
				printf("\n");
				free(tmp_string);
				break;
			case ARTIO_TYPE_FLOAT :
				tmp_float = (float *)malloc( length * sizeof(float) );
				artio_parameter_get_float_array(handle, key, length, tmp_float);
				printf("%36s | %6s |", key, "FLOAT"); 
				for ( i = 0; i < length; i++ ) {
					printf(" %e", tmp_float[i] );
				}
				printf("\n");
				free(tmp_float);
				break;
			case ARTIO_TYPE_DOUBLE :
				tmp_double = (double *)malloc( length * sizeof(double) );
				artio_parameter_get_double_array(handle, key, length, tmp_double);

				printf("%36s | %6s |", key, "DOUBLE"); 
				for ( i = 0; i < length; i++ ) {
					printf(" %e", tmp_double[i] );
				}
				printf("\n");
				free(tmp_double);
				break;
			case ARTIO_TYPE_INT :
				tmp_int = (int *)malloc( length * sizeof(int) );
				artio_parameter_get_int_array(handle, key, length, tmp_int);

				printf("%36s | %6s |", key, "INT"); 
				for ( i = 0; i < length; i++ ) {
					printf(" %d", tmp_int[i] );
				}
				printf("\n");
				free(tmp_int);
				break;
			case ARTIO_TYPE_LONG :
				tmp_long = (int64_t *)malloc( length * sizeof(int64_t) );
				artio_parameter_get_long_array(handle, key, length, tmp_long);

				printf("%36s | %6s |", key, "LONG"); 
				for ( i = 0; i < length; i++ ) {
					printf(" %ld", tmp_long[i] );
				}
				printf("\n");
				free(tmp_long);
				break;
			default :
				fprintf(stderr, "ERROR: unknown ARTIO type %d\n", type );
				exit(1);
		}
	}

	artio_fileset_close(handle);
}
