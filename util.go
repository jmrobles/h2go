/*
Copyright 2020 JM Robles (@jmrobles)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
