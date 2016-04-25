package main

import (
	"fmt"
	"log"
	"os"

	artio "github.com/phil-mansfield/go-artio"
)

func PrintHeader(prefix string) error {
	handle, err := artio.FilesetOpen(prefix, 0, artio.NullContext)
	if err != nil { return err }

	for key, pType, n, res := artio.ParameterIterate(handle);
		res == artio.Success;
		key, pType, n, res = artio.ParameterIterate(handle) {

		fmt.Println(key, pType, n)
	}

	return nil
}

func main() {
	if len(os.Args) != 1 {
		fmt.Println("Usage: ./print_header fileset_prefix")
	}

	if err := PrintHeader(os.Args[1]); err != nil {
		log.Fatal(err.Error())
	}
}