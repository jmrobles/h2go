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
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

// Using a testing pattern similar to Go MySQL Driver (https://github.com/go-sql-driver/mysql)
// Main testing entry point
var (
	user      string
	pass      string
	addr      string
	dbname    string
	inMem     bool
	dsn       string
	available bool
)

type dbTest struct {
	*testing.T
	conn *sql.DB
}

func (dt dbTest) checkErr(err error) {
	if err != nil {
		dt.Errorf("error: %s", err)
	}
}

func init() {
	env := func(key, defVal string) string {
		if val := os.Getenv(key); val != "" {
			return val
		}
		return defVal
	}
	user = env("H2_TEST_USER", "sa")
	pass = env("H2_TEST_PASSWORD", "")
	addr = env("H2_TEST_ADDR", "h2server:9092")
	dbname = env("H2_TEST_DBNAME", "test")
	inMemS := env("H2_TEST_IN_MEMORY", "true")
	inMem, err := strconv.ParseBool(inMemS)
	if err != nil {
		inMem = true
	}
	if pass != "" {
		dsn = fmt.Sprintf("h2://%s:%s@%s/%s?mem=%t", user, pass, addr, dbname, inMem)
	} else {
		dsn = fmt.Sprintf("h2://%s@%s/%s?mem=%t", user, addr, dbname, inMem)
	}
	// Check alive
	log.Printf(">>> addr: %s", addr)
	c, err := net.Dial("tcp", addr)
	if err == nil {
		available = true
		c.Close()
	} else {
		log.Printf("Can't connect: %s", err)
	}
}
func runTests(t *testing.T, tests ...func(dt *dbTest)) {
	var err error
	if !available {
		t.Errorf("H2 Server not running on %s", addr)
	}
	conn, err := sql.Open("h2", dsn)
	if err != nil {
		t.Fatalf("Can't connect to the H2 server: %s", err)
	}
	defer conn.Close()
	db := &dbTest{t, conn}
	for _, test := range tests {
		test(db)
		conn.Exec("DROP TABLE IF EXISTS test")

	}

}
func TestPing(t *testing.T) {
	runTests(t, func(dt *dbTest) {
		err := dt.conn.Ping()
		dt.checkErr(err)
	})
}

func TestSimpleCRUD(t *testing.T) {
	runTests(t, func(dt *dbTest) {
		var err error
		// Create table
		sent := "CREATE TABLE test (id int, name varchar, age int)"
		_, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		// Insert a row
		sent = "INSERT INTO test VALUES (1, 'Paco', 23)"
		result, err := dt.conn.Exec(sent)
		dt.checkErr(err)
		nR, err := result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
		// Query
		sent = "SELECT * FROM test"
		rows, err := dt.conn.Query(sent)
		dt.checkErr(err)
		for rows.Next() {
			var (
				id   int
				name string
				age  int
			)
			err = rows.Scan(&id, &name, &age)
			dt.checkErr(err)
			if id != 1 {
				dt.Errorf("ID mismatch (not equal to 1)")
			}
			if name != "Paco" {
				dt.Errorf("Name mismatch (not equal to 'Paco')")
			}
			if age != 23 {
				dt.Errorf("Age mismatch (not equal to 23)")
			}
		}
		err = rows.Close()
		// Update row
		sent = "UPDATE test SET age = 24 WHERE id = 1"
		result, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		nR, err = result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows updated not equal to 1")
		}
		// Query again
		sent = "SELECT * FROM test"
		rows, err = dt.conn.Query(sent)
		dt.checkErr(err)
		for rows.Next() {
			var (
				id   int
				name string
				age  int
			)
			err = rows.Scan(&id, &name, &age)
			dt.checkErr(err)
			if id != 1 {
				dt.Errorf("ID mismatch (not equal to 1)")
			}
			if name != "Paco" {
				dt.Errorf("Name mismatch (not equal to 'Paco')")
			}
			if age != 24 {
				dt.Errorf("Age mismatch (not equal to 24)")
			}
		}
		err = rows.Close()
		// Insert another row
		sent = "INSERT INTO test VALUES (2, 'John', 24)"
		result, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		nR, err = result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
		// Delete all
		sent = "DELETE FROM test"
		result, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		nR, err = result.RowsAffected()
		dt.checkErr(err)
		if nR != 2 {
			dt.Errorf("Num rows deleted not equal to 2")
		}
		// Skip DROP TABLE (done by the wrapper)
	})
}

func TestCRUDwithParameters(t *testing.T) {
	runTests(t, func(dt *dbTest) {
		var err error
		var sent string
		var (
			id   int    = 1
			name string = "Paco"
			age  int    = 23
		)
		// Create table
		sent = "CREATE TABLE test (id int, name varchar, age int)"
		_, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		// Insert with parameters
		sent = "INSERT INTO test VALUES (?, ?, ?)"
		result, err := dt.conn.Exec(sent, id, name, age)
		dt.checkErr(err)
		nR, err := result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
		// Query
		sent = "SELECT * FROM test"
		rows, err := dt.conn.Query(sent)
		dt.checkErr(err)
		for rows.Next() {
			var (
				id   int
				name string
				age  int
			)
			err = rows.Scan(&id, &name, &age)
			dt.checkErr(err)
			if id != 1 {
				dt.Errorf("ID mismatch (not equal to 1)")
			}
			if name != "Paco" {
				dt.Errorf("Name mismatch (not equal to 'Paco')")
			}
			if age != 23 {
				dt.Errorf("Age mismatch (not equal to 23)")
			}
		}
		err = rows.Close()
	})
}

func TestDateTimeTypes(t *testing.T) {
	runTests(t, func(dt *dbTest) {
		var err error
		var sent string
		var (
			id      int = 1
			dtFixed time.Time
		)
		// Create table
		sent = "CREATE TABLE test (id INT, t TIME, ttz TIME WITH TIME ZONE, d DATE, ts TIMESTAMP, tstz TIMESTAMP WITH TIME ZONE)"
		_, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		// Insert a row
		sent = "INSERT INTO test VALUES (?, ?, ?, ?, ?, ?)"
		loc, err := time.LoadLocation("Europe/Madrid")
		if err != nil {
			dt.Skipf("Can't get timezone for Europe/Madrid: %s", err)
		}
		dtFixed = time.Date(2020, 5, 25, 9, 1, 2, 123, loc)
		result, err := dt.conn.Exec(sent, id, dtFixed, dtFixed, dtFixed, dtFixed, dtFixed)
		dt.checkErr(err)
		nR, err := result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
		// Query
		sent = "SELECT * FROM test"
		rows, err := dt.conn.Query(sent)
		dt.checkErr(err)
		for rows.Next() {
			var (
				vTime        time.Time
				vTimeTZ      time.Time
				vDate        time.Time
				vTimestamp   time.Time
				vTimestampTZ time.Time
			)
			err = rows.Scan(&id, &vTime, &vTimeTZ, &vDate, &vTimestamp, &vTimestampTZ)
			dt.checkErr(err)
			if id != 1 {
				dt.Errorf("ID mismatch (not equal to 1)")
			}
			// TIME check
			if vTime.Hour() != 9 || vTime.Minute() != 1 || vTime.Second() != 2 {
				dt.Errorf("Time mismatch: %d %d %d", vTime.Hour(), vTime.Minute(), vTime.Second())
			}
			// TIME WITH TIME ZONE check
			_, nSecOffset := vTimeTZ.Zone()
			if vTimeTZ.Hour() != 9 || vTimeTZ.Minute() != 1 || vTimeTZ.Second() != 2 || nSecOffset != 7200 {
				dt.Errorf("Time TZ mismatch: %d %d %d %d", vTimeTZ.Hour(), vTimeTZ.Minute(), vTimeTZ.Second(), nSecOffset)
			}
			// DATE check
			if vDate.Day() != 25 || vDate.Month() != 5 || vDate.Year() != 2020 {
				dt.Errorf("Date mismatch: %d %d %d", vDate.Day(), vDate.Month(), vDate.Year())
			}
			// TIMESTAMP check
			if vTimestamp.Day() != 25 || vTimestamp.Month() != 5 || vTimestamp.Year() != 2020 || vTimestamp.Hour() != 9 || vTimestamp.Minute() != 1 || vTimestamp.Second() != 2 {
				dt.Errorf("Timestamp mismatch: %d %d %d %d %d %d", vTimestamp.Day(), vTimestamp.Month(), vTimestamp.Year(), vTimestamp.Hour(), vTimestamp.Minute(), vTimestamp.Second())
			}
			// TIMESTAMP WITH TIME Zone check
			_, nSecOffset = vTimeTZ.Zone()
			if vTimestampTZ.Day() != 25 || vTimestampTZ.Month() != 5 || vTimestampTZ.Year() != 2020 || vTimestampTZ.Hour() != 9 || vTimestampTZ.Minute() != 1 || vTimestampTZ.Second() != 2 || nSecOffset != 7200 {
				dt.Errorf("Timestamp TZ mismatch: %d %d %d %d %d %d %d", vTimestampTZ.Day(), vTimestampTZ.Month(), vTimestampTZ.Year(), vTimestampTZ.Hour(), vTimestampTZ.Minute(), vTimestampTZ.Second(), nSecOffset)
			}
		}
		err = rows.Close()
	})
}

func TestOtherTypes(t *testing.T) {
	runTests(t, func(dt *dbTest) {
		var err error
		var sent string
		// CREATE TABLE
		sent = "CREATE TABLE test (id INT, name VARCHAR(100), height FLOAT, isGood BOOLEAN, numAtoms DOUBLE, age SMALLINT)"
		_, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		// INSERT
		var (
			id       int     = 1
			name     string  = "Paco"
			height   float32 = 1.88
			isGood   bool    = true
			numAtoms float64 = 13213123332132.5
			age      int16   = 23
		)
		sent = "INSERT INTO test VALUES (?, ?, ?, ?, ?, ?)"
		result, err := dt.conn.Exec(sent, id, name, height, isGood, numAtoms, age)
		dt.checkErr(err)
		nR, err := result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
		// Query
		sent = "SELECT * FROM test"
		rows, err := dt.conn.Query(sent)
		dt.checkErr(err)
		for rows.Next() {
			err = rows.Scan(&id, &name, &height, &isGood, &numAtoms, &age)
			dt.checkErr(err)
			if id != 1 {
				dt.Errorf("ID mismatch (not equal to 1)")
			}
			if name != "Paco" {
				dt.Errorf("Name mismatch (not equal to 'Paco')")
			}
			if height != 1.88 {
				dt.Errorf("Height mismatch (not equal to 1.88)")
			}
			if !isGood {
				dt.Errorf("isGood is false")
			}
			if numAtoms != 13213123332132.5 {
				dt.Errorf("Num atoms mismatch (not equal to 13213123332132.5)")
			}
			if age != 23 {
				dt.Errorf("Age mismatch (not equal to 23)")
			}
		}
		rows.Close()
	})
}

func TestStmt(t *testing.T) {
	runTests(t, func(dt *dbTest) {
		var err error
		var sent string
		// CREATE TABLE
		sent = "CREATE TABLE test (id INT, name VARCHAR(100))"
		_, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		// Get Stmt
		stmt, err := dt.conn.Prepare("INSERT INTO test VALUES (?,?)")
		dt.checkErr(err)
		result, err := stmt.Exec(1, "Paco")
		dt.checkErr(err)
		nR, err := result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
	})
}

func TestTx(t *testing.T) {
	runTests(t, func(dt *dbTest) {
		var err error
		var sent string
		// CREATE TABLE
		sent = "CREATE TABLE test (id INT, name VARCHAR(100))"
		_, err = dt.conn.Exec(sent)
		dt.checkErr(err)
		// TX with commit
		tx, err := dt.conn.Begin()
		dt.checkErr(err)
		result, err := tx.Exec("INSERT INTO test VALUES (1, 'Paco')")
		dt.checkErr(err)
		nR, err := result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
		err = tx.Commit()
		dt.checkErr(err)
		// Query
		var (
			id   int
			name string
		)
		sent = "SELECT * FROM test"
		rows, err := dt.conn.Query(sent)
		dt.checkErr(err)
		for rows.Next() {
			err = rows.Scan(&id, &name)
			dt.checkErr(err)
			if id != 1 {
				dt.Errorf("ID mismatch (not equal to 1)")
			}
			if name != "Paco" {
				dt.Errorf("Name mismatch (not equal to 'Paco')")
			}
		}
		rows.Close()
		// Tx with rollback
		tx, err = dt.conn.Begin()
		dt.checkErr(err)
		result, err = tx.Exec("INSERT INTO test VALUES (2, 'John')")
		dt.checkErr(err)
		nR, err = result.RowsAffected()
		dt.checkErr(err)
		if nR != 1 {
			dt.Errorf("Num rows inserted not equal to 1")
		}
		err = tx.Rollback()
		dt.checkErr(err)
		// Query
		sent = "SELECT * FROM test"
		rows, err = dt.conn.Query(sent)
		dt.checkErr(err)
		for rows.Next() {
			err = rows.Scan(&id, &name)
			dt.checkErr(err)
			if id != 1 {
				dt.Errorf("ID mismatch (not equal to 1)")
			}
			if name != "Paco" {
				dt.Errorf("Name mismatch (not equal to 'Paco')")
			}
		}
		rows.Close()
	})
}
