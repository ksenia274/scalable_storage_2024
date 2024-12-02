package main

import (
	"testing"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)

	foundZero := false
	foundOne := false

	for _, v := range b {
		if v == zero {
			foundZero = true
		}
		if v == one {
			foundOne = true
		}
	}

	if !foundZero {
		t.Errorf("Expected to find '%c' in the slice, but it was not found.", zero)
	}

	if !foundOne {
		t.Errorf("Expected to find '%c' in the slice, but it was not found.", one)
	}
}
