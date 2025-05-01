package syro

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func TestLogger(t *testing.T) {
	t.Run("test-log-creation", func(t *testing.T) {
		log := NewLog(ERROR, "qweqwe", "my-source", "my-event", "my-event-id")

		decoded, err := decodeStructToStrings(log)
		if err != nil {
			t.Fatal(err)
		}

		// parse the created_at field from the json string and check it the time is
		// within the last 2 seconds
		type parsed struct {
			CreatedAt time.Time `json:"time" bson:"time"`
		}

		t.Run("test json unmarshalling", func(t *testing.T) {
			if err := stringIncludes(decoded.JSON, []string{
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
			if err := json.Unmarshal([]byte(decoded.JSON), &v); err != nil {
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

		t.Run("test-bson-unmarshal", func(t *testing.T) {
			if err := stringIncludes(decoded.BSON, []string{
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

type decodedStrings struct {
	JSON string
	BSON string
}

// Run the struct through json and bson marshalers and return the results as strings.
func decodeStructToStrings(v any) (*decodedStrings, error) {
	json, err := json.Marshal(&v)
	if err != nil {
		return nil, err
	}

	bson, err := bson.MarshalExtJSON(&v, false, false)
	if err != nil {
		return nil, err
	}

	return &decodedStrings{
		JSON: string(json),
		BSON: string(bson)}, nil
}
