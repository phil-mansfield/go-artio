package main

import (
	"fmt"
	"os"
	"log"
	"strconv"

	artio "github.com/phil-mansfield/go-artio"
)

// TODO: Move this into artio.go
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
	fileIndices := getIntOrLong(h, h.Key("particle_file_sfc_index"))
	masses := h.GetFloat(h.Key("particle_species_mass"))

	speciesIndices := make([]int64, len(counts) + 1)
	for i := range counts {
		speciesIndices[i+1] = speciesIndices[i] + counts[i]
	}

	err = h.OpenParticles()
	if err != nil { return err }
	defer h.CloseParticles()

	err = h.ParticleCacheSfcRange(fileIndices[0], fileIndices[1] - 1)
	if err != nil { return err }
	defer h.ParticleClearSfcCache()

	primary := make([]float64, h.GetInt(h.Key("num_primary_variables"))[0])
	secondary := make([]float32, h.GetInt(h.Key("num_secondary_variables"))[0])
	id, subspecies, err := h.ReadParticle(primary, secondary)
	if err != nil { return err }
	fmt.Println(id, subspecies)
	fmt.Println(primary)
	fmt.Println(counts, masses)

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
