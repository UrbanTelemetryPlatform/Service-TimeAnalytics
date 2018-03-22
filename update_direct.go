package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"encoding/json"

	_ "github.com/lib/pq"
)

func updateLivedataDirect(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	fmt.Println("Update live data request")

	if r.Method != "POST" {
		fmt.Fprint(w, "Only POST requests allowed")
		fmt.Println("Only POST requests")
		w.WriteHeader(403)
	}

	//Read body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("Could not understand JSON: %v", err)
		fmt.Println(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var input livedata
	err = json.Unmarshal(body, &input)
	if err != nil {
		msg := fmt.Sprintf("Could not understand JSON: %v", err)
		fmt.Println(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	stmt := "INSERT INTO livedata (segmentid, time, speed) VALUES ($1,$2,$3) "
	stmt += "ON CONFLICT (segmentid) DO UPDATE SET speed = $3, time = $2 WHERE livedata.segmentid = $1"
	_, err = db.Exec(stmt, input.SEGMENTID, input.TIME, input.SPEED)
	if err != nil {
		msg := fmt.Sprintf("Could not insert data: %v", err)
		fmt.Println(msg)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

}
