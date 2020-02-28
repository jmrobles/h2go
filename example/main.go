package main

import (
	"database/sql"
	"log"

	_ "github.com/jmrobles/h2go"
)

func main() {
	log.Printf("H2GO Example")

	conn, err := sql.Open("h2", "h2://sa@localhost/jander?mem=true")
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	rows, err := conn.Query("SELECT 1+2 AS ping, 'paco'")
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
	for rows.Next() {
		err := rows.Scan(&value, &name)
		if err != nil {
			log.Printf("Can't get value: %s", err)
			continue
		}
		log.Printf("Value: %d - Name: %s", value, name)
	}
	rows.Close()
	log.Printf("Done")

}
