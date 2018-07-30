package main

import (
	"context"
	"log"

	"cloud.google.com/go/bigquery"
	"fmt"
		"golang.org/x/oauth2/google"
	"golang.org/x/oauth2"
	bq "google.golang.org/api/bigquery/v2"
	"encoding/hex"
	"crypto/rand"
	)

var (
	projectID   = "fresh-8-testing"
	dataSetName = "lab_lee"
	tableName = "array_test_inferred_1532985867"//fmt.Sprintf("array_test_inferred_%v", time.Now().Unix())
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	//newHotness()

	oldAndBusted()
}

func oldAndBusted() {
	client, err := google.DefaultClient(oauth2.NoContext, bq.BigqueryInsertdataScope)
	if err != nil {
		log.Fatalf("%v", err)
	}

	service, err := bq.New(client)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// passes, expected
	oldPutItem(service, []Item{{AnArray: []NumRecord{{Number: 42}}}})
	fmt.Println("42 passed")

	// passes, expected
	oldPutItem(service, []Item{{AnArray: make(NumRecords,0)}})
	fmt.Println("empty passed")

	// fail, expected
	oldPutItem(service, []Item{{AnArray: nil}})
	fmt.Println("nil passed")
}

func oldPutItem(service *bq.Service, items []Item) {
	rows := make([]*bq.TableDataInsertAllRequestRows, 0, len(items))
	for _, item := range items {
		rows = append(rows, &bq.TableDataInsertAllRequestRows{
			Json: map[string]bq.JsonValue{
				"an_array": item.AnArray,
			},
		})
	}

	req := &bq.TableDataInsertAllRequest{
		Rows: rows,
	}

	tableDataService := bq.NewTabledataService(service)

	call := tableDataService.InsertAll(projectID, dataSetName, tableName, req)
	resp, err := call.Do()
	if err != nil {
		log.Fatalf("%v", err)
	}

	for _, insErr := range resp.InsertErrors {
		for _, err := range insErr.Errors {
			log.Printf("%v", err)
		}
	}

	if len(resp.InsertErrors) > 0 {
		log.Fatalf("want len(resp.InsertErrors) = 0, got %v", len(resp.InsertErrors))
	}
}

func uuid() (string, error) {
	var u [16]byte
	_, err := rand.Read(u[:])
	if err != nil {
		return "", err
	}

	u[8] = (u[8] | 0x80) & 0xBF
	u[6] = (u[6] | 0x40) & 0x4F

	return hex.EncodeToString(u[:]), nil
}

func newHotness() {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("%v", err)
	}

	myDataset := client.Dataset(dataSetName)

	table := myDataset.Table(tableName)

	schema, err := bigquery.InferSchema(Item{})
	if err != nil {
		log.Printf("%v", err)
	}

	err = table.Create(ctx, &bigquery.TableMetadata{Schema: schema})
	if err != nil {
		log.Fatalf("%v", err)
	}

	u := table.Uploader()
	u.IgnoreUnknownValues = true

	// passes as expected
	putItem(ctx, u, []Item{{AnArray: []NumRecord{{Number: 42}}}})
	fmt.Println("42 passed")

	// fails unexpectedly
	putItem(ctx, u, []Item{{AnArray: make(NumRecords,0)}})
	fmt.Println("empty passed")

	// fails as expected
	putItem(ctx, u, []Item{{AnArray: nil}})
	fmt.Println("nil passed")
}

func putItem(ctx context.Context, u *bigquery.Uploader, items []Item) {
	err := u.Put(ctx, items)
	if err != nil {
		log.Println(err)
		putMultiErrors := err.(bigquery.PutMultiError)
		for _, rowInsertionError := range putMultiErrors {
			log.Printf("%v", rowInsertionError.Error())
			for _, e := range rowInsertionError.Errors {
				log.Printf("%v", e)
			}
		}
		log.Fatal("failed to upload data")
	}
}

type NumRecord struct {
	Number int64 `bigquery:"number"`
}

type Item struct {
	AnArray NumRecords `bigquery:"an_array"`
}

type NumRecords []NumRecord