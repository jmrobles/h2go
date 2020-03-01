package h2go

import (
	"crypto/sha256"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
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

// L Log if apply
func L(level log.Level, text string, args ...interface{}) {
	if !doLogging {
		return
	}
	log.StandardLogger().Logf(level, text, args...)
}
