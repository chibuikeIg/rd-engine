package core

import (
	"io/fs"
	"os"
	"strconv"

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

	segments, err := s.Segments()
	if err != nil {
		return 1, err
	}

	segmentId := 1

	if len(segments) > 0 {
		segmentId = len(segments)
		activeFile := segments[segmentId-1]
		if activeFile.Size() >= config.MFS {
			segmentId += 1
		}
	}

	return segmentId, nil
}

func (s *Segment) CreateSegment(segmentID int, flag int) (*os.File, error) {
	formatedSegmentID := "0" + strconv.Itoa(segmentID)
	file := config.SegmentStorageBasePath + "/" + formatedSegmentID + ".data.txt"
	// Create a single instance of file
	f, err := os.OpenFile(file, flag, 0644)
	return f, err
}
