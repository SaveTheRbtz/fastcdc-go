// Command fastcdc prints FastCDC chunk offsets and sizes for a file.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/SaveTheRbtz/fastcdc-go"
)

const kiB = 1024
const miB = 1024 * kiB

var errInvalidFlags = errors.New("invalid flags")

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		switch {
		case errors.Is(err, flag.ErrHelp):
			return
		case errors.Is(err, errInvalidFlags):
			os.Exit(2)
		}
		_, _ = fmt.Fprintln(os.Stderr, "fastcdc:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) (err error) {
	fs := flag.NewFlagSet("fastcdc", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fileName := fs.String("file", "", "input file (required)")
	avgSize := fs.Int("avg", 1*miB, "average chunk size")
	minSize := fs.Int("min", 0, "minimum chunk size (default avg / 4)")
	maxSize := fs.Int("max", 0, "maximum chunk size (default avg * 4)")
	normalization := fs.Int("normalization", 1, "normalization level: -1 (none), 1, 2, or 3")
	csvOutput := fs.Bool("csv", false, "output as CSV (default false)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return flag.ErrHelp
		}
		return fmt.Errorf("%w: %v", errInvalidFlags, err)
	}
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

	headerFormat := "%9s  %9s\n"
	rowFormat := "%9d  %9d\n"
	offsetHeader, sizeHeader := "OFFSET", "SIZE"
	if *csvOutput {
		headerFormat = "%s,%s\n"
		rowFormat = "%d,%d\n"
		offsetHeader, sizeHeader = "Offset", "Size"
	}
	if _, err := fmt.Fprintf(stdout, headerFormat, offsetHeader, sizeHeader); err != nil {
		return fmt.Errorf("write output: %w", err)
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
		if _, err := fmt.Fprintf(stdout, rowFormat, offset, len(chunk)); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
}
