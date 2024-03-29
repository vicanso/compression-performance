package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/andybalholm/brotli"
	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
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
	buf := make([]byte, lz4.CompressBlockBound(len(data)))
	n, err := lz4.CompressBlock(data, buf, nil)
	if err != nil {
		return nil, err
	}
	buf = buf[:n]
	return buf, nil
}

func doLZ4Decode(buf []byte) ([]byte, error) {
	// 直接选择10倍大小
	dst := make([]byte, 10*len(buf))
	n, err := lz4.UncompressBlock(buf, dst)
	if err != nil {
		return nil, err
	}
	dst = dst[:n]
	return dst, nil
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

var encoder, _ = zstd.NewWriter(nil)

func doZSTDEncode(data []byte, level int) ([]byte, error) {
	buf := encoder.EncodeAll(data, make([]byte, 0, len(data)))
	return buf, nil
}

func doZSTDDecode(buf []byte) ([]byte, error) {
	d, err := zstd.NewReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	dst := &bytes.Buffer{}
	defer d.Close()

	_, err = io.Copy(dst, d)
	if err != nil {
		return nil, err
	}
	d.Close()
	return dst.Bytes(), nil
}

func doS2Encode(data []byte, level int) ([]byte, error) {
	dst := &bytes.Buffer{}
	enc := s2.NewWriter(dst)
	err := enc.EncodeBuffer(data)
	if err != nil {
		enc.Close()
		return nil, err
	}
	// Blocks until compression is done.
	err = enc.Close()
	if err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}

func doS2Decode(buf []byte) ([]byte, error) {
	dec := s2.NewReader(bytes.NewReader(buf))
	dst := &bytes.Buffer{}
	_, err := io.Copy(dst, dec)
	if err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
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
		{
			Name:       "s2",
			Compress:   doS2Encode,
			Decompress: doS2Decode,
		},
		{
			Name:       "zstd",
			Compress:   doZSTDEncode,
			Decompress: doZSTDDecode,
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
