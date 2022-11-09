package quickbolt

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// getEndianType returns the endian type of the host.
func getEndianType() (binary.ByteOrder, error) {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		return binary.LittleEndian, nil
	case [2]byte{0xAB, 0xCD}:
		return binary.BigEndian, nil
	default:
		return nil, fmt.Errorf("could not determine endian type of system")
	}
}

func toBytes(u uint64) ([]byte, error) {
	buf := make([]byte, 8)

	eType, err := getEndianType()
	if err != nil {
		return nil, fmt.Errorf("error while getting endian type: %w", err)
	}

	switch eType {
	case binary.LittleEndian:
		binary.LittleEndian.PutUint64(buf, u)
	case binary.BigEndian:
		binary.BigEndian.PutUint64(buf, u)
	}

	return buf, nil
}
