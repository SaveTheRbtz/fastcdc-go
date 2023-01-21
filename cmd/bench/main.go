package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"

	"github.com/SaveTheRbtz/fastcdc-go"
	jotfsCDC "github.com/jotfs/fastcdc-go"
	"github.com/restic/chunker"
)

const kiB = 1024
const miB = 1024 * kiB

var fileName = flag.String("file", "", "input file (required)")
var avgSize = flag.Int("avg", 2*miB, "average chunk size")
var minSize = flag.Int("min", 0, "minimum chunk size. (default avg / 4)")
var maxSize = flag.Int("max", 0, "maximum chunk size (default avg * 4)")

func Restic(r io.Reader) ([]int, error) {
	minChunkSize := *minSize
	if minChunkSize == 0 {
		minChunkSize = *avgSize / 4
	}
	maxChunkSize := *maxSize
	if maxChunkSize == 0 {
		maxChunkSize = *avgSize * 4
	}

	cnkr := chunker.NewWithBoundaries(r, chunker.Pol(0x3DA3358B4DC173), uint(minChunkSize), uint(maxChunkSize))
	cnkr.SetAverageBits(int(math.Log2(float64(*avgSize))))

	buf := make([]byte, 8*miB)
	chunkSizes := make([]int, 0, 1000)
	for {
		c, err := cnkr.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error reading chunk: %w", err)
		}
		chunkSizes = append(chunkSizes, len(c.Data))
	}
	return chunkSizes, nil
}

func FastCDC(r io.Reader) ([]int, error) {
	cnkr, err := fastcdc.NewChunker(r, fastcdc.Options{
		MinSize:     *minSize,
		MaxSize:     *maxSize,
		AverageSize: *avgSize,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating chunker: %w", err)
	}

	chunkSizes := make([]int, 0, 1000)
	for {
		c, err := cnkr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error reading chunk: %w", err)
		}
		chunkSizes = append(chunkSizes, c.Length)
	}

	return chunkSizes, nil
}

func OrigFastCDC(r io.Reader) ([]int, error) {
	cnkr, err := jotfsCDC.NewChunker(r, jotfsCDC.Options{
		MinSize:     *minSize,
		MaxSize:     *maxSize,
		AverageSize: *avgSize,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating chunker: %w", err)
	}

	chunkSizes := make([]int, 0, 1000)
	for {
		c, err := cnkr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error reading chunk: %w", err)
		}
		chunkSizes = append(chunkSizes, c.Length)
	}

	return chunkSizes, nil
}

func main() {
	flag.Parse()
	if *fileName == "" {
		log.Fatal("flag -file is required")
	}

	for name, f := range map[string]func(io.Reader) ([]int, error){
		"restic":      Restic,
		"fastcdc":     FastCDC,
		"origfastcdc": OrigFastCDC,
	} {

		r, err := os.Open(*fileName)
		if err != nil {
			log.Fatalf("unable to open file: %v", err)
		}
		defer r.Close()

		startTime := time.Now()
		chunkSizes, err := f(r)
		if err != nil {
			log.Fatalf("unable to read chunks: %s: %v", name, err)
		}
		// TODO: add chunk distribution stats
		log.Printf("%s: %d chunks in %s", name, len(chunkSizes), time.Since(startTime))
	}
}
