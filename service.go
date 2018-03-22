package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/lib/pq"
	"google.golang.org/appengine"

	"github.com/jmoiron/sqlx"
)

type output struct {
	success bool
	error   string
	data    interface{}
}

type livedata struct {
	SEGMENTID int64
	TIME      string
	SPEED     int64
}

var db *sqlx.DB
var datastoreName string

func main() {

	datastoreName = os.Getenv("POSTGRES_CONNECTION")

	var err error
	db, err = sqlx.Connect("postgres", datastoreName)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Ensure the table exists. Running an SQL query also checks the connection to the PostgreSQL server
	if err := createTables(); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/api/processAll", processAll)
	http.HandleFunc("/api/processWeekdayHours", processWeekdayHours)
	http.HandleFunc("/api/readWeekdayHours", readWeekdayHours)
	http.HandleFunc("/", getStatus)
	appengine.Main()
}

func createTables() error {
	err := prepareWeekdayHours()
	if err != nil {
		return err
	}

	return nil
}

func getStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "This service is running \n")
}

func processAll(w http.ResponseWriter, r *http.Request) {

}
