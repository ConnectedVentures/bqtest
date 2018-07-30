package main

import (
	"context"
	"log"

	"cloud.google.com/go/bigquery"
	"fmt"
	"time"
)

var (
	projectID   = "fresh-8-testing"
	dataSetName = "lab_lee"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("%v", err)
	}

	myDataset := client.Dataset(dataSetName)

	table := myDataset.Table(fmt.Sprintf("array_test_inferred_%v", time.Now().Unix()))

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