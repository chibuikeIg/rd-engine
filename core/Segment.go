package core

import (
	"io/fs"
	"os"
	"strconv"
	"strings"

	"reversed-database.engine/config"
)

type Segment struct {
}

func NewSegment() *Segment {
	return &Segment{}
}

func (s *Segment) Segments() ([]fs.FileInfo, error) {
	// Reads storage directories
	dirEntries, err := os.ReadDir(config.SegmentStorageBasePath)
	if err != nil {
		return nil, err
	}

	segments := []fs.FileInfo{}

	for _, de := range dirEntries {
		if !de.IsDir() {
			fileInfo, err := de.Info()
			if err != nil {
				return nil, err
			}
			segments = append(segments, fileInfo)
		}
	}

	return segments, nil
}

func (s *Segment) GetActiveSegmentID() (int, error) {

	segmentId := 1
	segments, err := s.Segments()

	if err != nil {
		return 0, err
	}

	if len(segments) > 0 {

		activeFile := segments[len(segments)-1]
		fName := activeFile.Name()
		activeFNameSlice := strings.SplitN(fName, ".", 3)
		segmentId, err := strconv.Atoi(activeFNameSlice[0])
		if err != nil {
			return 0, err
		}
		if activeFile.Size() >= config.MFS {
			segmentId += 1
		}
	}

	return segmentId, nil
}

func (s *Segment) CreateSegment(segmentID int) (*os.File, error) {
	formatedSegmentID := "0" + strconv.Itoa(segmentID)
	file := config.SegmentStorageBasePath + "/" + formatedSegmentID + ".data.txt"
	// Create a single instance of file
	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC, 0644)
	return f, err
}
