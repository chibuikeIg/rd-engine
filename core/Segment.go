package core

import (
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// Maximum file/segment size
var mfs = 3025

type Segment struct {
	SegmentPaths string
}

func NewSegment(segmentsPath string) *Segment {
	return &Segment{segmentsPath}
}

func (s *Segment) Segments() ([]fs.FileInfo, error) {
	// Reads storage directories
	dirEntries, err := os.ReadDir(s.SegmentPaths)
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

func (s *Segment) GetActiveSegmentID() (string, error) {

	activeSeg := "01"
	segments, err := s.Segments()

	if err != nil {
		return "", err
	}

	if len(segments) > 0 {

		activeFile := segments[len(segments)-1]
		activeSeg = activeFile.Name()
		activeFNameSlice := strings.SplitN(activeSeg, ".", 3)
		id, err := strconv.Atoi(activeFNameSlice[0])
		if err != nil {
			return "", err
		}
		fmt.Println(activeFile.Size())
		if activeFile.Size() >= int64(mfs) {
			id += 1
		}
		activeSeg = "0" + strconv.Itoa(id)
	}

	return activeSeg, nil
}
