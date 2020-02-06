package pgconn

import (
    "errors"
    "io"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestIsConnectError(t *testing.T) {
    err := &connectError{msg: "-"}
    assert.True(t, IsConnectError(err))

    assert.False(t, IsConnectError(io.EOF))
}

func TestIsConnLockError(t *testing.T) {
    err := &connLockError{status: "-"}
    assert.True(t, IsConnLockError(err))

    assert.False(t, IsConnLockError(io.EOF))
}

func TestIsParseConfigError(t *testing.T) {
    err := &parseConfigError{msg: "-"}
    assert.True(t, IsParseConfigError(err))

    assert.False(t, IsParseConfigError(io.EOF))
}

func TestIsPgConnError(t *testing.T) {
    err := &pgconnError{msg: "-"}
    assert.True(t, IsPgConnError(err))

    assert.False(t, IsPgConnError(io.EOF))
}

func TestIsContextAlreadyDoneError(t *testing.T) {
    err := &contextAlreadyDoneError{err: errors.New("-")}
    assert.True(t, IsContextAlreadyDoneError(err))

    assert.False(t, IsContextAlreadyDoneError(io.EOF))
}

func TestIsWriteError(t *testing.T) {
    err := &writeError{err: errors.New("-")}
    assert.True(t, IsWriteError(err))

    assert.False(t, IsWriteError(io.EOF))
}
