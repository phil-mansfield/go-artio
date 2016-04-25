package main

import (
	"fmt"
	"log"
	"os"

	artio "github.com/phil-mansfield/go-artio"
)

func PrintHeader(prefix string) error {
	h, err := artio.FilesetOpen(prefix, 0, artio.NullContext)
	if err != nil { return err }

	for key, iter := h.Iterate(); iter; key, iter = h.Iterate() {
		f := "%36s | %6s |%v"
		switch key.Type {
		case artio.String: continue; fmt.Printf(f, key.Name, "String", h.GetString(key))
		case artio.Float:  fmt.Printf(f, key.Name, "Float",  h.GetFloat(key))
		case artio.Double: fmt.Printf(f, key.Name, "Double", h.GetDouble(key))
		case artio.Int:    continue; fmt.Printf(f, key.Name, "Int",    h.GetInt(key))
		case artio.Long:   continue; fmt.Printf(f, key.Name, "Long",   h.GetLong(key))
		default: return fmt.Errorf("Unrecognized ARTIO type.")
		}
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: ./print_header fileset_prefix")
	}

	if err := PrintHeader(os.Args[1]); err != nil { log.Fatal(err.Error()) }
}