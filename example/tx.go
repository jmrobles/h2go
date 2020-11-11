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
