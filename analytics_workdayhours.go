package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

const tableNameWeekdayHours = "analytics_weekdayhours"

func prepareWeekdayHours() error {
	stmt := `CREATE TABLE IF NOT EXISTS ` + tableNameWeekdayHours + ` (
		segmentid  		INTEGER,
		weekday	   		INTEGER,
		hour			INTEGER,
		timezone    	INTEGER,
		count			INTEGER,
		average_speed 	DOUBLE PRECISION
	)`
	_, err := db.Exec(stmt)
	return err
}

func processWeekdayHours(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	// Sets your Google Cloud Platform project ID.
	projectID := "utp-md"

	// Creates a client.
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	tablename := "`utp.traffic_data`"
	//Query the table
	stmt := `SELECT
		SEGMENTID AS segmentid,
		EXTRACT(DAYOFWEEK FROM TIME) AS weekday,
		EXTRACT(HOUR FROM TIME) -5 AS hour,
		-5  AS timezone,
		COUNT(*) AS count,
		AVG(SPEED) AS average_speed
  		FROM ` + tablename + `
   		GROUP BY SEGMENTID, WEEKDAY, HOUR`

	//Execute Query
	query := client.Query(stmt)
	it, err := query.Read(ctx)

	if err != nil {
		msg := fmt.Sprintf("Could not retrieve columns: %v", err)
		log.Fatal(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	//results := make([]map[string]interface{}, 0)

	//Loop Results
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}

		if err != nil {
			msg := fmt.Sprintf("Could not retrieve columns: %v", err)
			log.Fatal(msg)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}

		entry := make(map[string]interface{})
		entry["segmentid"] = row[0]
		entry["weekday"] = row[1]
		entry["hour"] = row[2]
		entry["timezone"] = row[3]
		entry["count"] = row[4]
		entry["average_speed"] = row[5]
		//results = append(results, entry)

		insertIntoTable(tableNameWeekdayHours, "segmentid,weekday,hour,timezone,count,average_speed", ":segmentid,:weekday,:hour,:timezone,:count,:average_speed", entry)
	}

	fmt.Fprintf(w, "Processing successful \n")

}

func readWeekdayHours(w http.ResponseWriter, r *http.Request) {

	response, err := readTable(tableNameWeekdayHours, "*", "")
	if err != nil {
		msg := fmt.Sprintf("Read table failed: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, response)
}
