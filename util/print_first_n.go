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

	err = h.OpenParticles()
	if err != nil { return err }
	defer h.CloseParticles()

	err = h.ParticleCacheSfcRange(0, fileIndices[len(fileIndices) - 1] - 1)
	if err != nil { return err }
	defer h.ParticleClearSfcCache()

	roots := h.GetLong(h.Key("num_root_cells"))[0]
	numSpeciesBuf := make([]int32, len(counts))
	numParticlesRead := 0

	primarySizes := h.GetInt(h.Key("num_primary_variables"))
	secondarySizes := h.GetInt(h.Key("num_secondary_variables"))
	primaryBufs := make([][]float64, len(primarySizes))
	for i := range primaryBufs {
		primaryBufs[i] = make([]float64, primarySizes[i])
	}
	secondaryBufs := make([][]float32, len(secondarySizes))
	for i := range secondaryBufs {
		secondaryBufs[i] = make([]float32, secondarySizes[i])
	}
	
RootLoop:
	for root := int64(0); root < roots; root++ {
		err = h.ParticleReadRootCellBegin(root, numSpeciesBuf)
		if err != nil { return err }
		
		for species := 0; species < len(numSpeciesBuf); species++ {
			h.ParticleReadSpeciesBegin(species)
			if err != nil { return err }

			for i := int32(0); i < numSpeciesBuf[species]; i++ {
				id, _, err := h.ReadParticle(
					primaryBufs[species], secondaryBufs[species],
				)
				if err != nil { return err }
				fmt.Printf("%10d: X: %8.3g V: %9.3g\n", id,
					primaryBufs[species][:3],
					primaryBufs[species][3:6])
				numParticlesRead++
				if numParticlesRead == n { break RootLoop }
			}

			err = h.ParticleReadSpeciesEnd()
			if err != nil { return err }
		}
		
		err = h.ParticleReadRootCellEnd()
		if err != nil { return err }
	}

	fmt.Println(masses)

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
