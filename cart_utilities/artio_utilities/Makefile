CC = gcc
CFLAGS = -O2 -g -Wall
LIBS = -lm
INCLUDES =

all: artio_print_header artio_validate artio_remap

artio_print_header: artio_print_header.c ../artio/*.c
	$(CC) $(CFLAGS) -I. -I../artio/ $(INCLUDES) \
		../artio/*.c \
		artio_print_header.c \
		-o artio_print_header \
		$(LIBS)

artio_validate: artio_validate.c ../artio/*.c
	$(CC) $(CFLAGS) -I. -I../artio/ $(INCLUDES) \
		../artio/*.c \
		artio_validate.c \
		-o artio_validate \
		$(LIBS)

artio_remap: artio_remap.c ../artio/*.c
	$(CC) $(CFLAGS) -I. -I../artio/ \
		-DARTIO_REMAP_POSIX \
		$(INCLUDES) \
        ../artio/*.c \
        artio_remap.c \
        -o artio_remap \
        $(LIBS)

clean:
	rm -f artio_print_header artio_validate artio_remap
