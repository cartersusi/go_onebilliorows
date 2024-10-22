package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/mmap"
)

type Station struct {
	total float32
	count uint16
	min   float32
	max   float32
}
type fixed_string [37]byte
type StationMap map[fixed_string]Station

type ChunkReader struct {
	data []byte
	pos  int
}

func NewChunkReader(data []byte) *ChunkReader {
	return &ChunkReader{data: data}
}

func (r *ChunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func processLine(line string, stations StationMap) {
	var station fixed_string
	var measurement float32
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == ';' {
			copy(station[:i], line[:i])
			tmp, _ := strconv.ParseFloat(line[i+1:], 32)
			measurement = float32(tmp)
			break
		}
	}

	s, ok := stations[station]
	if !ok {
		s = Station{}
	}
	s.total += measurement
	s.count++
	s.min = min(s.min, measurement)
	s.max = max(s.max, measurement)
	stations[station] = s
}

func processChunk(chunk []byte) StationMap {
	stations := make(StationMap)

	scanner := bufio.NewScanner(NewChunkReader(chunk))
	lineNumber := 1
	for scanner.Scan() {
		processLine(scanner.Text(), stations)
		lineNumber++
	}
	return stations
}

func readLargeFile(filePath string, chunkSize int) (StationMap, error) {
	reader, _ := mmap.Open(filePath)
	defer reader.Close()

	fileInfo, _ := os.Stat(filePath)

	fileSize := fileInfo.Size()

	var wg sync.WaitGroup
	res := make(chan StationMap)

	for offset := int64(0); offset < fileSize; {
		remainingSize := fileSize - offset
		currentChunkSize := int64(chunkSize)
		if remainingSize < currentChunkSize {
			currentChunkSize = remainingSize
		}

		largeChunk := make([]byte, currentChunkSize+1024)
		n, _ := reader.ReadAt(largeChunk, offset)

		actualChunkSize := int64(n)
		if idx := strings.LastIndexByte(string(largeChunk[:n]), '\n'); idx >= 0 {
			actualChunkSize = int64(idx + 1)
		}

		chunk := largeChunk[:actualChunkSize]
		offset += actualChunkSize

		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			res <- processChunk(chunk)
		}(chunk)
	}

	go func() {
		wg.Wait()
		close(res)
	}()

	stations := make(StationMap)
	for re := range res {
		for station, mes := range re {
			s, ok := stations[station]
			if !ok {
				s = Station{}
			}
			s.total += mes.total
			s.count += mes.count
			s.min = float32(min(s.min, mes.min))
			s.max = float32(max(s.max, mes.max))
			stations[station] = s
		}
	}

	return stations, nil
}

func min(a, b float32) float32 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float32) float32 {
	if a > b {
		return a
	}
	return b
}

func main() {
	start_time := time.Now()
	fp := "data/measurements.txt"
	chunkSize := (64 * 1024 * 1024)

	stations, err := readLargeFile(fp, chunkSize)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	fmt.Println("Time taken: ", time.Since(start_time))

	f, err := os.Create("output.txt")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer f.Close()

	f.WriteString("Station,Mean,Min,Max\n")
	for station, s := range stations {
		_, err := f.WriteString(fmt.Sprintf("%s,%f,%f,%f\n", strings.TrimRight(string(station[:]), "\x00"), s.total/float32(s.count), s.min, s.max))
		if err != nil {
			log.Fatalf("Error writing to file: %v", err)
		}
	}

	fmt.Println("Done")
}
