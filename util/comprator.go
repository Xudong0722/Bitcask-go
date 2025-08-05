package util

import (
	"bytes"
	"fmt"
	"strings"
)

type Comprator func(a, b interface{}) (int, error)

func StringComprator(a, b interface{}) (int, error) {
	sa, ok := a.(string)
	if !ok {
		return 0, fmt.Errorf("a is not string.")
	}
	sb, ok := b.(string)
	if !ok {
		return 0, fmt.Errorf("b is not string.")
	}
	return strings.Compare(sa, sb), nil
}

func byteArrayComprator(a, b interface{}) int {
	ba := a.([]byte)
	bb := b.([]byte)
	return bytes.Compare(ba, bb)
}

func IntComprator(a, b interface{}) (int, error) {
	sa, ok := a.(int)
	if !ok {
		return 0, fmt.Errorf("a is not int.")
	}
	sb, ok := b.(int)
	if !ok {
		return 0, fmt.Errorf("b is not int.")
	}

	if sa == sb {
		return 0, nil
	}
	if sa < sb {
		return -1, nil
	}
	return 1, nil
}