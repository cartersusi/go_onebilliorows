package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/exp/mmap"
)

type Station struct {
	total float64
	count int
	min   float64
	max   float64
}
type StationMap map[string]Station

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
	parts := strings.Split(line, ";")

	station := parts[0]
	measurement, _ := strconv.ParseFloat(parts[1], 64)
	s, ok := stations[station]
	if !ok {
		s = Station{}
	}
	s.total += measurement
	s.count++
	s.min = math.Min(s.min, measurement)
	s.max = math.Max(s.max, measurement)
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
			s.min = math.Min(s.min, mes.min)
			s.max = math.Max(s.max, mes.max)
			stations[station] = s
		}
	}

	return stations, nil
}

func main() {
	fp := "large_dataset.txt"
	chunkSize := 64 * 1024 * 1024

	stations, err := readLargeFile(fp, chunkSize)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	f, err := os.Create("output.txt")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}
	defer f.Close()

	f.WriteString("Station,Mean,Min,Max\n")
	for station, s := range stations {
		_, err := f.WriteString(fmt.Sprintf("%s,%f,%f,%f\n", station, s.total/float64(s.count), s.min, s.max))
		if err != nil {
			log.Fatalf("Error writing to file: %v", err)
		}
	}

	fmt.Println("Done")
}
