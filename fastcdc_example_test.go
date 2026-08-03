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

	data := bytes.Repeat([]byte("content-defined storage\n"), 100)
	var saved [][]byte
	for _, chunk := range chunker.Chunks(data) {
		saved = append(saved, bytes.Clone(chunk))
	}
	fmt.Println(len(saved) > 1, bytes.Equal(bytes.Join(saved, nil), data))

	// Output:
	// true true
}

func ExampleReader() {
	chunker, err := fastcdc.New(fastcdc.Config{AverageSize: 256})
	if err != nil {
		panic(err)
	}

	data := bytes.Repeat([]byte("streamed input remains in order\n"), 100)
	reader := chunker.NewReader(bytes.NewReader(data))
	var restored bytes.Buffer
	for {
		offset := reader.InputOffset()
		chunk, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if offset != int64(restored.Len()) {
			panic("non-contiguous chunk")
		}
		restored.Write(chunk)
	}
	fmt.Println(bytes.Equal(restored.Bytes(), data))

	// Output:
	// true
}
