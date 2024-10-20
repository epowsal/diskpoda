package diskpoda

import (
	"encoding/binary"

	"linbo.ga/toolfunc"
)

func sizeToBytes(size uint64, bs []byte) (n int) {
	if size < (1 << 6) {
		bs[0] = byte(size)
		return 1
	} else if size < (1 << 14) {
		binary.BigEndian.PutUint16(bs[:2], uint16(size))
		toolfunc.ByteLeftSet(bs[0], 1, 2)
		return 2
	} else if size < (1 << 22) {
		binary.BigEndian.PutUint16(bs[:2], uint16(size>>8))
		bs[2] = byte(size)
		toolfunc.ByteLeftSet(bs[0], 2, 2)
		return 3
	} else if size < (1 << 30) {
		panic("Segment data too large")
		binary.BigEndian.PutUint32(bs[:4], uint32(size))
		toolfunc.ByteLeftSet(bs[0], 3, 2)
		return 4
	} else {
		panic("Segment data too large")
	}
}

func bytesToSize(bs []byte) (size, cnt uint64) {
	switch toolfunc.ByteLeftGet(bs[0], 2) {
	case 0:
		return uint64(bs[0] << 2 >> 2), 1
	case 1:
		return uint64(binary.BigEndian.Uint16(bs[:2]) << 2 >> 2), 2
	case 2:
		return uint64(binary.BigEndian.Uint16(bs[:2])<<2>>2)<<8 | uint64(bs[2]), 2
	case 3:
		return uint64(binary.BigEndian.Uint32(bs) << 2 >> 2), 4
	default:
		panic("error")
	}
}

func sizeLen(size uint64) (n uint64) {
	if size < (1 << 6) {
		return 1
	} else if size < (1 << 14) {
		return 2
	} else if size < (1 << 22) {
		return 3
	} else if size < (1 << 30) {
		return 4
	} else {
		panic("Segment data too large")
	}
	return 0
}

func DiskSizeStep(size uint64) uint64 {
	switch toolfunc.GetSizeBitCnt(size) {
	case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13:
		return 8
	case 14:
		return 16
	case 15:
		return 32
	case 16:
		return 64
	case 17:
		return 128
	case 18:
		return 256
	case 19:
		return 512
	case 20:
		return 1024
	case 21:
		return 2048
	default:
		return 4096
	}
}

func lbs(bs []byte) uint64 {
	return uint64(len(bs))
}

func cbs(bs []byte) uint64 {
	return uint64(cap(bs))
}
