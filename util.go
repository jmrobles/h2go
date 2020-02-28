package h2go

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"golang.org/x/text/encoding/unicode"
)

func getHashedPassword(username string, password string) ([32]byte, error) {
	payload := fmt.Sprintf("%s@%s", strings.ToUpper(username), password)
	data, err := unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewEncoder().Bytes([]byte(payload))
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}
