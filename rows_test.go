package ropdb

import (
	"testing"
)

func TestRows(t *testing.T) {
	rows := WrapRows(nil, nil)

	if rows.Scan() {
		t.Errorf("Scan should return false if inner rows is nil")
	}
}
