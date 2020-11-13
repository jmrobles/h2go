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
	"database/sql"
	"testing"
)

func TestConnection(t *testing.T) {
	_, err := sql.Open("h2", "h2://sa@h2server:9092/test?mem=true&logging=debug")
	if err != nil {
		t.Errorf("Can't connect to the server: %s", err)
	}
}
