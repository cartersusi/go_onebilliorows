package main

import (
	//"bufio"
	"fmt"
	"io"
	"log"
	"os"
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
type max_station [37]byte
type max_line [43]byte
type StationMap map[max_station]Station

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

func parseFloatFromBytes(b []byte) float32 {
	if b[0] == '-' {
		if b[2] == '.' {
			return -((float32(b[1] - '0')) + ((float32(b[3] - '0')) / 10))
		}
		return -((float32(b[1]-'0')*10 + float32(b[2]-'0')) + (float32(b[4]-'0') / 10))
	} else {
		if b[1] == '.' {
			return (float32(b[0]-'0') + (float32(b[2]-'0') / 10))
		}
		return (float32(b[0]-'0')*10 + float32(b[1]-'0')) + (float32(b[3]-'0') / 10)
	}
}

func processLine(line max_line, stations StationMap) {
	var station max_station
	var measurement float32
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == ';' {
			copy(station[:], line[:i])
			measurement = parseFloatFromBytes(line[i+1:])
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

	var fixed_line max_line
	fixed_line_counter := 0
	line_number := 1
	for _, c := range chunk {
		if c == '\n' {
			line_number++
			processLine(fixed_line, stations)
			fixed_line = [43]byte{}
			fixed_line_counter = 0
		} else {
			fixed_line[fixed_line_counter] = c
			fixed_line_counter++
		}
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
