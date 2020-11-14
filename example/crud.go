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

package main

import (
	"database/sql"
	"log"

	_ "github.com/jmrobles/h2go"
)

func main() {
	log.Printf("H2GO Example")

	conn, err := sql.Open("h2", "h2://sa@localhost/test?mem=true&logging=info")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	// Create table
	log.Printf("CREATE TABLE")
	ret, err := conn.Exec("CREATE TABLE test (id int not null, name varchar(100))")
	if err != nil {
		log.Printf("Can't execute sentence: %s", err)
		return
	}
	// Insert
	ret, err = conn.Exec("INSERT INTO test VALUES (?, ?)",
		1, "John")
	if err != nil {
		log.Printf("Can't execute sentence: %s", err)
		return
	}
	lastID, err := ret.LastInsertId()
	if err != nil {
		log.Printf("Can't get last ID: %s", err)
	}
	nRows, err := ret.RowsAffected()
	if err != nil {
		log.Printf("Can't get num rows: %s", err)
	}
	log.Printf("LastID: %d - NumRowsAffected: %d", lastID, nRows)
	// Query
	rows, err := conn.Query("SELECT * FROM test")
	if err != nil {
		log.Printf("Can't execute query: %s", err)
	}
	for rows.Next() {
		var (
			id   int
			name string
		)
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Printf("Can't scan values in row: %s", err)
			continue
		}
		log.Printf("Row: %d - %s", id, name)
	}
	rows.Close()
	// Update
	ret, err = conn.Exec("UPDATE test SET name = 'Juan' WHERE id = 1")
	if err != nil {
		log.Printf("Can't execute sentence: %s", err)
		return
	}
	lastID, err = ret.LastInsertId()
	if err != nil {
		log.Printf("Can't get last ID: %s", err)
	}
	nRows, err = ret.RowsAffected()
	if err != nil {
		log.Printf("Can't get num rows: %s", err)
	}
	log.Printf("LastID: %d - NumRowsAffected: %d", lastID, nRows)
	// Delete
	ret, err = conn.Exec("DELETE FROM test WHERE id = 1")
	if err != nil {
		log.Printf("Can't execute sentence: %s", err)
		return
	}
	lastID, err = ret.LastInsertId()
	if err != nil {
		log.Printf("Can't get last ID: %s", err)
	}
	nRows, err = ret.RowsAffected()
	if err != nil {
		log.Printf("Can't get num rows: %s", err)
	}
	log.Printf("LastID: %d - NumRowsAffected: %d", lastID, nRows)
	conn.Close()
	// time.Sleep(20 * time.Second)
	log.Printf("Done")
}
