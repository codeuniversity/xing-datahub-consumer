package models

import (
	"strconv"
)

// Model for exporting
type Model interface {
	ToCSVLine() string
	UnmarshalFrom([]byte) error
}

func arrayHelper(arr []int32) string {
	s := ""
	for _, e := range arr {
		if len(s) != 0 {
			s += string('|')
		}
		s += strconv.Itoa(int(e))
	}
	return s
}
