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
	"context"
	"database/sql"
	"log"

	_ "github.com/jmrobles/h2go"
)

func main() {
	log.Printf("H2GO Example")

	conn, err := sql.Open("h2", "h2://sa@localhost/jander?mem=true&logging=debug")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	// Create table
	stmt, err := conn.Prepare("CREATE TABLE test (id int)")
	if err != nil {
		log.Fatalf("Can't preparate: %s", err)
	}
	result, err := stmt.Exec()
	if err != nil {
		log.Fatalf("Can't execute exec: %s", err)
	}
	log.Printf("Result: %v", result)
	// Begin TX for INSERT
	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("Can't start tx: %s", err)
	}
	result, err = tx.ExecContext(ctx, "INSERT INTO test VALUES 10")
	if err != nil {
		log.Fatalf("Can't execute insert: %s", err)
	}
	// Commit
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}
	// Check values
	rows, err := conn.Query("SELECT * FROM test")
	if err != nil {
		log.Fatalf("Can't select: %s", err)
	}
	for rows.Next() {
		var v int
		err := rows.Scan(&v)
		if err != nil {
			log.Printf("Can't scan row")
			continue
		}
		log.Printf("Value: %d", v)
	}
	log.Printf("End tx")

}
