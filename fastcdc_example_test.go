package fastcdc_test

import (
	"bytes"
	"fmt"
	"io"

	fastcdc "github.com/SaveTheRbtz/fastcdc-go"
)

func ExampleChunker_Chunks() {
	chunker, err := fastcdc.New(fastcdc.Config{AverageSize: 256})
	if err != nil {
		panic(err)
	}

	data := make([]byte, 2048)
	for offset, chunk := range chunker.Chunks(data) {
		fmt.Printf("offset=%d size=%d\n", offset, len(chunk))
	}

	// Output:
	// offset=0 size=1024
	// offset=1024 size=1024
}

func ExampleReader() {
	chunker, err := fastcdc.New(fastcdc.Config{
		AverageSize:   256,
		Normalization: fastcdc.NormalizationNone,
	})
	if err != nil {
		panic(err)
	}

	data := make([]byte, 65)
	data[64] = 0xc0
	reader := chunker.NewReader(bytes.NewReader(data))
	for {
		offset := reader.InputOffset()
		chunk, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("offset=%d size=%d\n", offset, len(chunk))
	}

	// Output:
	// offset=0 size=64
	// offset=64 size=1
}
