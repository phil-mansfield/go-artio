package main

import (
	"fmt"
	"os"
	"log"
	"strconv"

	artio "github.com/phil-mansfield/go-artio"
)

func PrintFirstN(prefix string, n int) error {
	h, err := artio.FilesetOpen(prefix, 0, artio.NullContext)
	if err != nil { return err }
	defer h.Close()

	numSpecies := h.GetInt(h.Key("num_particle_species"))[0]
	var counts []int64
	if key := h.Key("particle_species_num"); key.Type == artio.Int {
		counts32 := h.GetInt(key)
		counts := make([]int64, len(counts32))
		for i := range counts { counts[i] = int64(counts32[i]) }
	} else {
		counts = h.GetLong(key)
	}
	fmt.Println(numSpecies, counts)

	return nil
}

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: ./print_first_n fileset_prefix n")
	}

	prefix := os.Args[1]
	n, err := strconv.Atoi(os.Args[2])
	if err != nil { log.Fatalf(err.Error()) }

	err = PrintFirstN(prefix, n)
	if err != nil { log.Fatalf(err.Error()) }
}
