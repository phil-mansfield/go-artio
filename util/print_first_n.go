package main

import (
	"fmt"
	"os"
	"log"
	"strconv"

	artio "github.com/phil-mansfield/go-artio"
)


func getIntOrLong(h artio.Fileset, key artio.Key) []int64 {
	if key.Type == artio.Int {
		out32 := h.GetInt(key)
		out := make([]int64, len(out32))
		for i := range out { out[i] = int64(out32[i]) }
		return out
	} else {
		return h.GetLong(key)
	}
}

func PrintFirstN(prefix string, n int) error {
	h, err := artio.FilesetOpen(prefix, 0, artio.NullContext)
	if err != nil { return err }
	defer h.Close()

	var speciesNumKey artio.Key
	if h.HasKey("particle_species_num") {
		speciesNumKey = h.Key("particle_species_num")
	} else {
		speciesNumKey = h.Key("num_particles_per_species")
	}
	counts := getIntOrLong(h, speciesNumKey)
	fmt.Println(counts)

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
