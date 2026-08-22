package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var testT *testing.T

func SetT(t *testing.T) {
	testT = t
}

func NoErr[T any](v T, err error) T {
	require.NoError(testT, err)
	return v
}

func Err[T any](_ T, err error) error {
	require.Error(testT, err)
	return err
}

func NoErrB[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ErrB[T any](_ T, err error) error {
	if err == nil {
		panic("expected error")
	}
	return err
}
