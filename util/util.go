package util

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/lunixbochs/struc"
	"github.com/zxfishhack/fastdfs-go-client/proto"
	"io"
	"net"
	"time"
)

// #include <string.h>
import "C"

var (
	opts = &struc.Options{
		ByteAlign: 0,
		PtrSize:   8,
		Order:     binary.BigEndian,
	}
	ErrParam = errors.New("client check: param error")
)

type Reader interface {
	io.Reader
	Len() int
}

func GetErr(status int) error {
	return fmt.Errorf("server response status: %s", C.GoString(C.strerror(C.int(status))))
}

func Call(nc net.Conn, data interface{}, r Reader) (recvBuf []byte, status int, err error) {
	err = send(nc, data, r)
	if err != nil {
		return
	}
	return fdfsRecvResponse(nc)
}

func CallWithWriter(nc net.Conn, data interface{}, r Reader, w io.Writer) (status int, err error) {
	if w == nil {
		err = ErrParam
		return
	}
	err = send(nc, data, r)
	if err != nil {
		return
	}
	return fdfsRecvResponseWriter(nc, w)
}

func CallWithInterface(nc net.Conn, data interface{}, r Reader, res interface{}) (status int, err error) {
	err = send(nc, data, r)
	if err != nil {
		return
	}
	return fdfsRecvResponseInterface(nc, res)
}

func send(nc net.Conn, data interface{}, r Reader) (err error) {
	err = struc.PackWithOptions(nc, data, opts)
	if err != nil {
		if pc, ok := nc.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
		return
	}
	if r != nil {
		_, err = io.CopyN(nc, r, int64(r.Len()))
		if err != nil {
			if pc, ok := nc.(*pool.PoolConn); ok {
				pc.MarkUnusable()
			}
		}
	}
	return
}

func TimeoutCopyN(dst io.Writer, src io.Reader, timeout time.Duration, n int64) (err error) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	var nc int64
	for n > 0 && err == nil {
		select {
		case <-t.C:
			return context.DeadlineExceeded
		default:
		}
		nc, err = io.CopyN(dst, src, n)
		n -= nc
		if err == io.EOF {
			err = nil
		}
	}
	return
}

func TimeoutReadN(src io.Reader, timeout time.Duration, n int) (res []byte, err error) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	var nr int
	res = make([]byte, n)
	idx := 0
	for n > 0 && err == nil {
		select {
		case <-t.C:
			err = context.DeadlineExceeded
			return
		default:
		}
		nr, err = src.Read(res[idx:])
		n -= nr
		idx += nr
		if err == io.EOF {
			err = nil
		}
	}
	return
}

func fdfsRecvResponse(nc net.Conn) (recvBuf []byte, status int, err error) {
	header := &proto.Header{}
	err = struc.Unpack(nc, header)
	if err != nil {
		if pc, ok := nc.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
		return
	}
	status = header.Status
	if header.PkgLen == 0 {
		return
	}
	b := bytes.NewBuffer(nil)
	err = TimeoutCopyN(b, nc, time.Second*10, int64(header.PkgLen))
	recvBuf = b.Bytes()
	if err != nil {
		if pc, ok := nc.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
	}
	return
}

func fdfsRecvResponseWriter(nc net.Conn, w io.Writer) (status int, err error) {
	header := &proto.Header{}
	err = struc.Unpack(nc, header)
	if err != nil {
		if pc, ok := nc.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
		return
	}
	status = header.Status
	if header.PkgLen == 0 {
		return
	}
	err = TimeoutCopyN(w, nc, time.Second*10, int64(header.PkgLen))
	if err != nil {
		if pc, ok := nc.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
	}
	return
}

func fdfsRecvResponseInterface(nc net.Conn, res interface{}) (status int, err error) {
	header := &proto.Header{}
	err = struc.Unpack(nc, header)
	if err != nil {
		if pc, ok := nc.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
		return
	}
	status = header.Status
	if header.PkgLen == 0 {
		return
	}
	if status != 0 {
		if header.PkgLen > 0 {
			skip(nc, header.PkgLen)
		}
	}
	err = struc.Unpack(nc, res)
	return
}

func skip(nc net.Conn, sz int) {
	buf := make([]byte, sz)
	n, err := nc.Read(buf)
	if err != nil || n != sz {
		if pc, ok := nc.(*pool.PoolConn); ok {
			pc.MarkUnusable()
		}
	}
}
