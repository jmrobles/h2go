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
	"time"

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
	ret, err := conn.Exec("CREATE TABLE paco (id int not null, name varchar(100), height float, isMale boolean, numAtoms double, dob date, ts timestamp, tsz timestamp with time zone, start time, starttz time with time zone, age smallint)")
	if err != nil {
		log.Printf("Can't execute sentence: %s", err)
		return
	}
	// log.Printf("Ret: %v", ret)
	// lastID, err := ret.LastInsertId()
	// if err != nil {
	// 	log.Printf("Can't get last ID: %s", err)
	// }
	// nRows, err := ret.RowsAffected()
	// if err != nil {
	// 	log.Printf("Can't get num rows: %s", err)
	// }
	// log.Printf("LastID: %d - NumRowsAffected: %d", lastID, nRows)

	var numAtoms float64 = 123456789.0
	var age int16 = 16
	now := time.Now()
	ret, err = conn.Exec("INSERT INTO paco VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		26, "sander", 3.14, false, numAtoms, now, now, now, now, now, age)
	if err != nil {
		log.Printf("Can't execute sentence: %s", err)
		return
	}
	log.Printf("Ret: %v", ret)
	lastID, err := ret.LastInsertId()
	if err != nil {
		log.Printf("Can't get last ID: %s", err)
	}
	nRows, err := ret.RowsAffected()
	if err != nil {
		log.Printf("Can't get num rows: %s", err)
	}
	log.Printf("LastID: %d - NumRowsAffected: %d", lastID, nRows)

	ret, err = conn.Exec("INSERT INTO paco VALUES (100, 'paco', 1.51, false, 1.0, DATE '2019-01-01', TIMESTAMP '1970-01-01 00:00:01', TIMESTAMP WITH TIME ZONE '2005-12-01 10:59:59.123+02', TIME '10:20:30.123', TIME WITH TIME ZONE '10:20:30.123+02', 15)")
	if err != nil {
		log.Printf("Can't execute sentence: %s", err)
		return
	}
	log.Printf("Ret: %v", ret)
	lastID, err = ret.LastInsertId()
	if err != nil {
		log.Printf("Can't get last ID: %s", err)
	}
	nRows, err = ret.RowsAffected()
	if err != nil {
		log.Printf("Can't get num rows: %s", err)
	}
	log.Printf("LastID: %d - NumRowsAffected: %d", lastID, nRows)

	// ret, err = conn.Exec("DELETE FROM paco WHERE id = 100")
	// if err != nil {
	// 	log.Printf("Can't execute sentence: %s", err)
	// 	return
	// }
	// log.Printf("Ret: %v", ret)
	// lastID, err = ret.LastInsertId()
	// if err != nil {
	// 	log.Printf("Can't get last ID: %s", err)
	// }
	// nRows, err = ret.RowsAffected()
	// if err != nil {
	// 	log.Printf("Can't get num rows: %s", err)
	// }
	// log.Printf("LastID: %d - NumRowsAffected: %d", lastID, nRows)

	// rows, err := conn.Query("SELECT 1+2 AS ping, 'paco'")
	rows, err := conn.Query("SELECT * FROM paco")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		log.Printf("Can't get columns: %s", err)
	}
	log.Printf("Columns: %v", cols)
	var value int
	var name string
	var height float64
	var isMale bool
	var numAtoms2 float64
	var dob time.Time
	var ts time.Time
	var tsz time.Time
	var start time.Time
	var starttz time.Time
	for rows.Next() {
		log.Printf("ROWS")
		err := rows.Scan(&value, &name, &height, &isMale, &numAtoms2, &dob, &ts, &tsz, &start, &starttz, &age)
		if err != nil {
			log.Printf("Can't get value: %s", err)
			continue
		}
		log.Printf("Value: %d - Name: %s - Height: %f - Is Male: %v - Atoms: %f - Dob: %s - TS: %s - TSZ: %s - Time: %s TimeTZ: %s - Age: %d",
			value, name, height, isMale, numAtoms2, dob, ts, tsz, start, starttz, age)
	}
	rows.Close()
	conn.Close()
	// time.Sleep(20 * time.Second)
	log.Printf("Done")
}
