package syro

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestLogger(t *testing.T) {
	t.Run("test-log-creation", func(t *testing.T) {
		log := NewLog(ERROR, "qweqwe", "my-source", "my-event", "my-event-id")

		// decoded, err := decodeStructToStrings(log)
		// if err != nil {
		// 	t.Fatal(err)
		// }

		decodedJSON, err := json.Marshal(log)
		if err != nil {
			t.Fatal(err)
		}

		jsonStr := string(decodedJSON)

		// parse the created_at field from the json string and check it the time is
		// within the last 2 seconds
		type parsed struct {
			CreatedAt time.Time `json:"time" bson:"time"`
		}

		t.Run("test json unmarshalling", func(t *testing.T) {
			if err := stringIncludes(jsonStr, []string{
				`"level":4`,
				`message":"qweqwe"`,
				`"source":"my-source"`,
				`"event":"my-event"`,
				`"event_id":"my-event-id"`,
				`"time":`,
			}); err != nil {
				t.Fatal(err)
			}

			var v parsed
			if err := json.Unmarshal([]byte(jsonStr), &v); err != nil {
				t.Fatal(err)
			}

			if v.CreatedAt.Before(time.Now().Add(-2 * time.Second)) {
				t.Fatal("The time time is not within the last 2 seconds")
			}

			// Check the timezone of the created_at field
			if v.CreatedAt.Location().String() != "UTC" {
				t.Fatal("The created_at time is not in UTC")
			}
		})

		t.Run("test-string-method", func(t *testing.T) {
			logger := NewConsoleLogger(nil)
			str := log.String(logger)
			fmt.Printf("str: %v\n", str)

			now := time.Now().UTC()
			formattedTime := now.Format("2006-01-02 15:04:05")
			// NOTE: not sure if this will fail in some cases when running
			// remove the last 3 characters (seconds) from the formatted time
			formattedTime = formattedTime[:len(formattedTime)-3]
			if err := stringIncludes(str, []string{
				"error",
				"my-source",
				"my-event",
				"qweqwe",
				formattedTime, // check if the printed time is the same as the current time,
			}); err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("test-console-logger", func(t *testing.T) {
		if NewConsoleLogger(nil).GetProps().Settings != nil {
			t.Fatal("Settings should be nil")
		}

		_log := NewConsoleLogger(nil)
		_log.Info("my-message", LogFields{
			"field1": "val1",
			"field2": "val2",
		})

		if NewConsoleLogger(nil).SetEvent("my-event").GetProps().Event != "my-event" {
			t.Fatal("SetEvent failed")
		}

		lg := NewConsoleLogger(nil).
			SetSource("my-source").
			SetEventID("my-event-id")

		if lg.GetProps().Source != "my-source" && lg.GetProps().EventID != "my-event-id" {
			t.Fatal("SetEventID failed")
		}

		logExists, err := NewConsoleLogger(nil).LogExists(nil)
		if err == nil {
			t.Fatal("LogExists should always return an error")
		}

		if logExists {
			t.Fatal("LogExists should always return false")
		}

		if err.Error() != "method cannot be used with ConsoleLogger" {
			t.Fatal("LogExists should always return a predefined error")
		}
	})
}

func TestErrGroup(t *testing.T) {
	t.Run("test-new-errgroup", func(t *testing.T) {
		eg := NewErrGroup()
		if eg == nil {
			t.Fatal("New() should not return nil")
		}

		if len(*eg) != 0 {
			t.Fatal("New() should initialize an empty ErrGroup")
		}
	})

	t.Run("test-add-error", func(t *testing.T) {
		eg := NewErrGroup()
		err1 := errors.New("first error")
		err2 := errors.New("second error")

		eg.Add(err1)
		if len(*eg) != 1 {
			t.Fatal("Add() did not properly add the first error")
		}

		eg.Add(err2)
		if len(*eg) != 2 {
			t.Fatal("Add() did not properly add the second error")
		}

		eg.Add(nil) // test adding nil error
		if len(*eg) != 2 {
			t.Fatal("Add() should not add nil errors")
		}
	})

	t.Run("test-errors", func(t *testing.T) {
		eg := NewErrGroup()
		err1 := errors.New("first error")
		err2 := errors.New("second error")

		eg.Add(err1)
		eg.Add(err2)

		expected := "first error; second error"
		if eg.Error() != expected {
			t.Fatalf("Error() returned %q, want %q", eg.Error(), expected)
		}

		eg = NewErrGroup() // test with no errors
		if eg.Error() != "" {
			t.Fatalf("Error() should return an empty string for an empty ErrGroup, got %q", eg.Error())
		}
	})

	t.Run("test-len", func(t *testing.T) {
		eg := NewErrGroup()
		if eg.Len() != 0 {
			t.Fatalf("Len() should return 0 for a new ErrGroup, got %d", eg.Len())
		}

		eg.Add(errors.New("first error"))
		if eg.Len() != 1 {
			t.Fatalf("Len() should return 1 after adding one error, got %d", eg.Len())
		}

		eg.Add(nil) // adding nil should not change the count
		if eg.Len() != 1 {
			t.Fatalf("Len() should still return 1 after adding nil, got %d", eg.Len())
		}
	})
}

func TestMongoLogger(t *testing.T) {

	url := "mongodb://localhost:27017"

	const (
		dbName = "test"
	)

	opt := options.Client().
		SetMaxPoolSize(20).                   // Set the maximum number of connections in the connection pool
		SetMaxConnIdleTime(10 * time.Minute). // Close idle connections after the specified time
		ApplyURI(url)

	conn, err := mongo.Connect(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}

	defer conn.Disconnect(context.Background())

	t.Run("test-bson-unmarshalling", func(t *testing.T) {
		log := NewLog(ERROR, "qweqwe", "my-source", "my-event", "my-event-id")

		decodedBson, err := bson.MarshalExtJSON(&log, false, false)
		if err != nil {
			t.Fatal(err)
		}

		bsonStr := string(decodedBson)
		fmt.Printf("bsonStr: %v\n", bsonStr)

		if err := stringIncludes(bsonStr, []string{
			`"time":{"$date":`,
			`message":"qweqwe"`,
			`"source":"my-source"`,
			`"event":"my-event"`,
			`"event_id":"my-event-id"`,
		}); err != nil {
			t.Fatal(err)
		}

		bsonBytes, err := bson.Marshal(log)
		if err != nil {
			t.Fatal(err)
		}

		var parsedLog Log
		if err := bson.Unmarshal(bsonBytes, &parsedLog); err != nil {
			t.Fatalf("BSON Unmarshal failed with error: %v", err)
		}

		if parsedLog.Time.Before(time.Now().Add(-2 * time.Second)) {
			t.Fatal("The created_at time is not within the last 2 seconds")
		}
	})

	t.Run("test log creation", func(t *testing.T) {
		coll := conn.Database(dbName).Collection("test_syro_mongo_logger")
		// Remove the previous data
		if err := coll.Drop(context.Background()); err != nil {
			t.Fatal(err)
		}

		logger := NewMongoLogger(coll, nil)
		if logger == nil {
			t.Error("NewMongoLogger should not return nil")
		}

		if err := logger.Debug("qwe"); err != nil {
			t.Fatal(err)
		}

		// find the log in the collection
		var log Log
		if err := coll.FindOne(context.Background(), bson.M{}).Decode(&log); err != nil {
			t.Fatal(err)
		}

		if log.Message != "qwe" {
			t.Fatal("The log message should be 'qwe'")
		}

		if log.Level != DEBUG {
			t.Fatal("The log level should be ", DEBUG)
		}

		if log.Source != "" {
			t.Fatal("The log source should be empty")
		}

		if log.Source != "" {
			t.Fatal("The log source should be empty")
		}

		if log.Event != "" {
			t.Fatal("The log event should be empty")
		}

		if log.EventID != "" {
			t.Fatal("The log event_id should be empty")
		}

		// if the time is not within the last 2 seconds
		if log.Time.Before(time.Now().Add(-2 * time.Second)) {
			t.Fatal("The created_at time is not within the last 2 seconds")
		}
	})

	t.Run("test log fields", func(t *testing.T) {
		coll := conn.Database(dbName).Collection("test_mongo_logger_with_fields")
		if err := coll.Drop(context.Background()); err != nil {
			t.Fatal(err)
		}

		logger := NewMongoLogger(coll, nil)

		var asd error

		if err := logger.Debug("qwe", LogFields{
			"key1": "value1",
			"key2": 123,
			"asd":  asd,
		}); err != nil {
			t.Fatal(err)
		}

		var log Log
		if err := coll.FindOne(context.Background(), bson.M{}).Decode(&log); err != nil {
			t.Fatal(err)
		}

		fmt.Printf("log.Fields: %v\n", log.Fields)
		for k, v := range log.Fields {
			fmt.Printf("k: %-10v v: %-10v type: %-10T\n", k, v, v)
		}

		// test if the expected fields are in the log
		if log.Fields["key1"] != "value1" {
			t.Error("The key1 field should be 'value1', got: ", log.Fields["key1"])
		}

		// NOTE: i'm not sure what to do in this case, tests fail without the int32 type
		if log.Fields["key2"] != int32(123) {
			t.Error("The key2 field should be 123, got: ", log.Fields["key2"])
		}

		if log.Fields["asd"] != nil {
			t.Error("The asd field should be the same as the asd variable")
		}
	})

	t.Run("test log creation", func(t *testing.T) {

		coll := conn.Database(dbName).Collection("test_mongo_logger_with_source")
		if err := coll.Drop(context.Background()); err != nil {
			t.Fatal(err)
		}

		logger := NewMongoLogger(coll, nil).SetEventID("my-event-id")

		if err := logger.Info("my unique info event"); err != nil {
			t.Fatal(err)
		}

		t.Run("check if a created log exists", func(t *testing.T) {
			filter := bson.M{"event_id": "my-event-id"}
			exists, err := logger.LogExists(filter)
			if err != nil {
				t.Fatal(err)
			}

			if !exists {
				t.Fatal("The log should exist")
			}
		})

		t.Run("check if a non existent log does not exitst", func(t *testing.T) {
			filter := bson.M{"event_id": "this does not exist"}
			exists, err := logger.LogExists(filter)
			if err != nil {
				t.Fatal(err)
			}

			if exists {
				t.Fatal("The log should not exist")
			}
		})
	})

	t.Run("test find logs", func(t *testing.T) {
		coll := conn.Database(dbName).Collection("test_mongo_logger_find_logs")
		if err := coll.Drop(context.Background()); err != nil {
			t.Fatal(err)
		}

		msg := "this is a test"
		numLogs := 10

		logger := NewMongoLogger(coll, nil).SetEventID("my-event-id")
		for range numLogs {
			logger.Debug(msg)
		}

		// ---- test the find logs method ----
		test1, err := logger.FindLogs(LogFilter{
			TimeseriesFilter: TimeseriesFilter{Limit: 100, Skip: 0},
			EventID:          "my-event-id",
		}, 1000)

		if err != nil {
			t.Fatal(err)
		}

		if len(test1) != numLogs {
			t.Fatalf("The number of logs should be %v", numLogs)
		}

		// if all of the logs are not debug level and the data is not msg
		// then the test failed
		for _, log := range test1 {
			if log.Level != DEBUG || log.Message != msg {
				t.Fatal("The logs are not correct")
			}
		}

		// ---- test the find logs method with a limit ----
		test2, err := logger.FindLogs(LogFilter{
			EventID:          "my-event-id",
			TimeseriesFilter: TimeseriesFilter{Limit: 5, Skip: 0},
		}, 5)

		if err != nil {
			t.Fatal(err)
		}

		if len(test2) != 5 {
			t.Fatalf("The number of logs should be %v", 5)
		}

		// ---- other filters ----
		test3, err := logger.FindLogs(LogFilter{
			EventID:          "this-event-does-not-exist",
			TimeseriesFilter: TimeseriesFilter{Limit: 100, Skip: 0},
		}, 1_000)

		if err != nil {
			t.Fatal(err)
		}

		if len(test3) != 0 {
			t.Fatalf("The number of logs should be %v", 0)
		}
	})
}

// stringIncludes checks if the input string contains all of the strings in
// the array. If the input string does not contain a strings from the
// array, an error is returned.
func stringIncludes(s string, arr []string) error {
	for _, str := range arr {
		if !strings.Contains(s, str) {
			return fmt.Errorf("input string '%s' does not include '%s'", s, str)
		}
	}
	return nil
}

// type decodedStrings struct {
// 	JSON string
// 	BSON string
// }

// // Run the struct through json and bson marshalers and return the results as strings.
// func decodeStructToStrings(v any) (*decodedStrings, error) {
// 	json, err := json.Marshal(&v)
// 	if err != nil {
// 		return nil, err
// 	}

// bson, err := bson.MarshalExtJSON(&v, false, false)
// if err != nil {
// 	return nil, err
// }

// 	return &decodedStrings{
// 		JSON: string(json),
// 		BSON: string(bson)}, nil
// }
