package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/andybalholm/brotli"
	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
	"github.com/spf13/cobra"
)

type Compression struct {
	Name       string
	Compress   func([]byte, int) ([]byte, error)
	Decompress func([]byte) ([]byte, error)
	Level      int
}

func httpGet(url string) (data []byte, err error) {
	res, err := http.Get(url)
	if err != nil {
		return
	}
	defer res.Body.Close()
	data, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	return
}

func doGzip(buf []byte, level int) (data []byte, err error) {
	buffer := new(bytes.Buffer)

	w, _ := gzip.NewWriterLevel(buffer, level)
	_, err = w.Write(buf)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// doGunzip gunzip
func doGunzip(buf []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

func doBrEncode(buf []byte, level int) ([]byte, error) {
	buffer := new(bytes.Buffer)
	w := brotli.NewWriterLevel(buffer, level)
	defer w.Close()
	_, err := w.Write(buf)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
func doBrDecode(buf []byte) ([]byte, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	r := brotli.NewReader(bytes.NewBuffer(buf))
	return ioutil.ReadAll(r)
}

func doLZ4Encode(data []byte, level int) ([]byte, error) {
	dst := &bytes.Buffer{}
	w := lz4.NewWriter(dst)
	w.CompressionLevel = level
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}

func doLZ4Decode(buf []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(buf))
	return ioutil.ReadAll(r)
}

func doSnappyEncode(data []byte, level int) ([]byte, error) {
	dst := []byte{}
	dst = snappy.Encode(dst, data)
	return dst, nil
}

func doSnappyDecode(buf []byte) ([]byte, error) {
	var dst []byte
	return snappy.Decode(dst, buf)
}

func main() {

	count := 100
	requestURL := ""
	var rootCmd = &cobra.Command{
		Use: "compression",
	}
	rootCmd.Flags().IntVar(&count, "count", 100, "The count of test")
	rootCmd.Flags().StringVar(&requestURL, "url", "https://api.github.com/users/vicanso/repos", "The request url for test data")

	compressionList := []*Compression{
		{
			Name:       "gzip",
			Compress:   doGzip,
			Decompress: doGunzip,
			Level:      6,
		},
		{
			Name:       "gzip",
			Compress:   doGzip,
			Decompress: doGunzip,
			Level:      9,
		},
		{
			Name:       "br",
			Compress:   doBrEncode,
			Decompress: doBrDecode,
			Level:      6,
		},
		{
			Name:       "br",
			Compress:   doBrEncode,
			Decompress: doBrDecode,
			Level:      8,
		},
		{
			Name:       "br",
			Compress:   doBrEncode,
			Decompress: doBrDecode,
			Level:      10,
		},
		{
			Name:       "br",
			Compress:   doBrEncode,
			Decompress: doBrDecode,
			Level:      11,
		},
		{
			Name:       "lz4",
			Compress:   doLZ4Encode,
			Decompress: doLZ4Decode,
			Level:      0,
		},
		{
			Name:       "lz4",
			Compress:   doLZ4Encode,
			Decompress: doLZ4Decode,
			Level:      3,
		},
		{
			Name:       "snappy",
			Compress:   doSnappyEncode,
			Decompress: doSnappyDecode,
		},
	}
	err := rootCmd.Execute()
	if err != nil {
		panic(err)
	}

	data, err := httpGet(requestURL)
	if err != nil {
		panic(err)
	}

	rows := [][]string{
		{
			"Name",
			"Level",
			"Rate",
			"Compress",
			"Decompress",
			"Compress+Decompress",
		},
	}

	dataSize := float64(len(data))

	for _, c := range compressionList {
		var compressData []byte
		startedAt := time.Now()
		for i := 0; i < count; i++ {
			result, err := c.Compress(data, c.Level)
			if err != nil {
				panic(err)
			}
			if compressData == nil {
				compressData = result
			}
		}
		compressDuration := time.Since(startedAt)
		if len(compressData) == 0 {
			panic(errors.New("compress fail"))
		}
		startedAt = time.Now()
		var decompressData []byte
		for i := 0; i < count; i++ {
			result, err := c.Decompress(compressData)
			if err != nil {
				panic(err)
			}
			if decompressData == nil {
				decompressData = result
			}
		}
		if !bytes.Equal(data, decompressData) {
			panic(errors.New("decompress fail"))
		}
		ratio := 100 * float64(len(compressData)) / dataSize
		decompressDuration := time.Since(startedAt)
		row := []string{
			c.Name,
			strconv.Itoa(c.Level),
			strconv.FormatFloat(ratio, 'g', 3, 64) + "%",
			(compressDuration / time.Duration(count)).String(),
			(decompressDuration / time.Duration(count)).String(),
			((compressDuration + decompressDuration) / time.Duration(count)).String(),
		}
		rows = append(rows, row)
	}

	if err := ui.Init(); err != nil {
		panic(err)
	}
	defer ui.Close()
	compressionTable := widgets.NewTable()
	compressionTable.Rows = rows
	compressionTable.TextStyle = ui.NewStyle(ui.ColorWhite)
	compressionTable.SetRect(0, 0, 100, 2*len(rows)+1)

	ui.Render(compressionTable)

	uiEvents := ui.PollEvents()
	for {
		e := <-uiEvents
		switch e.ID {
		case "q", "<C-c>":
			return
		}
	}

}
