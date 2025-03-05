package checksum

import (
	"hash/crc32"
)

type CRC uint32

func ChecksumIEEE(data []byte) CRC {
	return CRC(crc32.ChecksumIEEE(data))
}
