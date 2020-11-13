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

	conn, err := sql.Open("h2", "h2://sa@localhost/jander?mem=true&logging=debug")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	// rows, err := conn.Query("SELECT 1+2 AS ping, 'paco'")
	// //rows, err := conn.Query("SELECT name FROM TableNotExists")
	// if err != nil {
	// 	log.Fatalf("ERROR: %s", err)
	// }
	// cols, err := rows.Columns()
	// if err != nil {
	// 	log.Printf("Can't get columns: %s", err)
	// }
	// log.Printf("Columns: %v", cols)
	// var value int
	// var name string
	// for rows.Next() {
	// 	err := rows.Scan(&value, &name)
	// 	if err != nil {
	// 		log.Printf("Can't get value: %s", err)
	// 		continue
	// 	}
	// 	log.Printf("Value: %d - Name: %s", value, name)
	// }
	// rows.Close()

	// Create table
	log.Printf("CREATE TABLE")
	//ret, err := conn.Exec("CREATE TABLE paco (id int not null, name varchar(100), height float, isMale boolean, numAtoms double, dob date, ts timestamp, tsz timestamp with time zone, start time, starttz time with time zone, age smallint)")
	stmt, err := conn.Prepare("SELECT 1+1")
	if err != nil {
		log.Fatalf("Can't preparate: %s", err)
	}
	rows, err := stmt.Query()
	if err != nil {
		log.Fatalf("Can't query: %s", err)
	}
	for rows.Next() {
		var v int32
		err := rows.Scan(&v)
		if err != nil {
			log.Fatalf("Can't scan: %s", err)
		}
		log.Printf("Row: %d", v)
	}

	// Exec
	stmt, err = conn.Prepare("CREATE TABLE test (id int)")
	if err != nil {
		log.Fatalf("Can't preparate: %s", err)
	}
	result, err := stmt.Exec()
	if err != nil {
		log.Fatalf("Can't execute exec: %s", err)
	}
	log.Printf("Result: %v", result)

	stmt, err = conn.Prepare("INSERT INTO test VALUES (?)")
	if err != nil {
		log.Fatalf("Can't preparate: %s", err)
	}
	v := 123
	result, err = stmt.Exec(v)
	if err != nil {
		log.Fatalf("Can't execute exec: %s", err)
	}
	log.Printf("Result: %v", result)
	// Select
	stmt, err = conn.Prepare("SELECT * FROM test")
	if err != nil {
		log.Fatalf("Can't preparate: %s", err)
	}
	rows, err = stmt.Query()
	if err != nil {
		log.Fatalf("Can't query: %s", err)
	}
	for rows.Next() {
		var v int32
		err := rows.Scan(&v)
		if err != nil {
			log.Fatalf("Can't scan: %s", err)
		}
		log.Printf("Row: %d", v)
	}

}
