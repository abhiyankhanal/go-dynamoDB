package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

//item structure to do crud
type Item struct {
	Year   int
	Title  string
	Plot   string
	Rating float64
}

//creating connection
func connectDynamo() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	return svc
}

//creating table
func createTable(tableName string, svc *dynamodb.DynamoDB) {

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Year"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("Title"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Year"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("Title"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(tableName),
	}

	_, err := svc.CreateTable(input)
	if err != nil {
		fmt.Printf("Got error calling CreateTable: %v", err)
	}

	fmt.Println("Created the table", tableName)
}

//create item
func putItem(tableName string, svc *dynamodb.DynamoDB, item Item) {

	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		log.Fatalf("Got error marshalling new movie item: %s", err)
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	_, err = svc.PutItem(input)
	if err != nil {
		log.Fatalf("Got error calling PutItem: %s", err)
	}

	year := strconv.Itoa(item.Year)

	fmt.Println("Successfully added '" + item.Title + "' (" + year + ") to table " + tableName)

}

//get method
func getItem(svc *dynamodb.DynamoDB, tableName string) (Item, error) {

	// snippet-start:[dynamodb.go.read_item.call]

	movieName := "The Big New Movie"
	movieYear := "2015"
	item := Item{}
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				N: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
	})
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	// snippet-end:[dynamodb.go.read_item.call]

	// snippet-start:[dynamodb.go.read_item.unmarshall]
	if result.Item == nil {
		msg := "Could not find '" + movieName + "'"
		return Item{}, errors.New(msg)
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	fmt.Println("Found item:")
	fmt.Println("Year:  ", item.Year)
	fmt.Println("Title: ", item.Title)
	fmt.Println("Plot:  ", item.Plot)
	fmt.Println("Rating:", item.Rating)
	// snippet-end:[dynamodb.go.read_item.unmarshall]
	return item, nil
}

//update method

func updateItem(svc *dynamodb.DynamoDB) {
	tableName := "Movies"
	movieName := "The Big New Movie"
	movieYear := "2015"
	movieRating := "0.5"

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				N: aws.String(movieRating),
			},
		},
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				N: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set Rating = :r"),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		log.Fatalf("Got error calling UpdateItem: %s", err)
	}

	fmt.Println("Successfully updated '" + movieName + "' (" + movieYear + ") rating to " + movieRating)
}

//delete method

func Delete(svc *dynamodb.DynamoDB, tableName string) {

	movieName := "The Big New Movie"
	movieYear := "2015"

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				N: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
		TableName: aws.String(tableName),
	}

	_, err := svc.DeleteItem(input)
	if err != nil {
		log.Fatalf("Got error calling DeleteItem: %s", err)
	}

	fmt.Println("Deleted '" + movieName + "' (" + movieYear + ") from table " + tableName)
}

func main() {
	table := "Movies"
	connection := connectDynamo()

	// av, err := getItem(connection, table)
	// fmt.Printf("Result is %v", av)
	// if err != nil {
	// 	fmt.Printf("Got an error: %v", err)
	// }
	item1 := Item{
		Year:   2017,
		Title:  "Hello",
		Plot:   "Action",
		Rating: 3.0,
	}
	item2 := Item{
		Year:   2018,
		Title:  "Hello World",
		Plot:   "Action",
		Rating: 3.0,
	}
	createTable(table, connection)
	putItem(table, connection, item1)
	putItem(table, connection, item2)
	Delete(connection, table)
	updateItem(connection)
	getItem(connection, table)
}
