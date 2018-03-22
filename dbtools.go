package main

import (
	"encoding/json"
	"fmt"
)

func readTable(table, fields, where string) (string, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()

	fmt.Println("Read live data request")

	stmt := "SELECT " + fields + " FROM " + table + " " + where
	rows, err := db.Queryx(stmt)
	if err != nil {
		return "", err
	}

	tableData := make([]map[string]interface{}, 0)

	for rows.Next() {
		entry := make(map[string]interface{})
		err := rows.MapScan(entry)
		if err != nil {
			return "", err
		}

		tableData = append(tableData, entry)
	}

	jsonString, err := json.Marshal(tableData)
	if err != nil {
		return "", err
	}

	return string(jsonString), nil

}

func insertIntoTable(table, fields, valuemap string, values map[string]interface{}) error {
	_, err := db.NamedExec("INSERT INTO "+table+" ("+fields+") VALUES ("+valuemap+")", values)
	return err
}

func clearTable(table, fields, valuemap string, values map[string]interface{}) error {
	_, err := db.Exec("DELETE FROM " + table)
	return err
}
