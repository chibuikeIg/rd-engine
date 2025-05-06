package utilities

import (
	"strconv"
	"strings"
)

// Gets segmentId as integer from file name
func GetSegmentIdFromFname(fn string) (int, error) {
	FNameSlice := strings.SplitN(fn, ".", 2)
	segmentId, err := strconv.Atoi(FNameSlice[0])
	if err != nil {
		return 0, err
	}
	return segmentId, nil
}
