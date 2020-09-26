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
	// log.Printf("CREATE TABLE")
	// ret, err := conn.Exec("CREATE TABLE public.paco (id int)")
	// if err != nil {
	// 	log.Printf("Can't execute sentence: %s", err)
	// 	return
	// }
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
	ret, err := conn.Exec("INSERT INTO public.paco VALUES (15), (20)")
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
	for rows.Next() {
		err := rows.Scan(&value)
		if err != nil {
			log.Printf("Can't get value: %s", err)
			continue
		}
		log.Printf("Value: %d - Name: %s", value, name)
	}
	rows.Close()
	conn.Close()
	// time.Sleep(20 * time.Second)
	log.Printf("Done")
}
