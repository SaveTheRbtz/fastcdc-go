// Command fastcdc prints FastCDC chunk offsets and sizes for a file.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/SaveTheRbtz/fastcdc-go"
)

const kiB = 1024
const miB = 1024 * kiB

var fileName = flag.String("file", "", "input file (required)")
var avgSize = flag.Int("avg", 1*miB, "average chunk size")
var minSize = flag.Int("min", 0, "minimum chunk size (default avg / 4)")
var maxSize = flag.Int("max", 0, "maximum chunk size (default avg * 4)")
var normalization = flag.Int("normalization", 1, "normalization level: -1 (none), 1, 2, or 3")
var csv = flag.Bool("csv", false, "output as CSV (default false)")

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fastcdc:", err)
		os.Exit(1)
	}
}

func run() (err error) {
	if *fileName == "" {
		return fmt.Errorf("flag -file is required")
	}
	f, err := os.Open(*fileName)
	if err != nil {
		return fmt.Errorf("open %q: %w", *fileName, err)
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()

	chunker, err := fastcdc.New(fastcdc.Config{
		AverageSize:   *avgSize,
		MinSize:       *minSize,
		MaxSize:       *maxSize,
		Normalization: fastcdc.Normalization(*normalization),
	})
	if err != nil {
		return err
	}
	reader := chunker.NewReader(f)

	if *csv {
		fmt.Printf("%s,%s\n", "Offset", "Size")
	} else {
		fmt.Printf("%9s  %9s\n", "OFFSET", "SIZE")
	}

	for {
		offset := reader.InputOffset()
		chunk, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if *csv {
			fmt.Printf("%d,%d\n", offset, len(chunk))
		} else {
			fmt.Printf("%9d  %9d\n", offset, len(chunk))
		}
	}
}
