package syro

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoLogger struct {
	Coll     *mongo.Collection
	Settings *LoggerSettings
	Source   string
	Event    string
	EventID  string
}

func NewMongoLogger(coll *mongo.Collection, settings *LoggerSettings) *MongoLogger {
	return &MongoLogger{Coll: coll, Settings: settings}
}

func (lg *MongoLogger) CreateIndexes() error {
	// return mongodb.NewIndexes().
	// 	Add("time", "level").
	// 	Add("source").
	// 	Add("event").
	// 	Add("event_id").
	// 	Create(lg.Coll)
	return nil
}

func (lg *MongoLogger) GetTableName() string {
	return lg.Coll.Name()
}

func (lg *MongoLogger) GetProps() LoggerProps {
	return LoggerProps{
		Settings: lg.Settings,
		Source:   lg.Source,
		Event:    lg.Event,
		EventID:  lg.EventID,
	}
}

func (lg *MongoLogger) Name() string {
	return "mongo"
}

func (lg *MongoLogger) SetSource(v string) Logger {
	lg.Source = v
	return lg
}

func (lg *MongoLogger) SetEvent(v string) Logger {
	lg.Event = v
	return lg
}

func (lg *MongoLogger) SetEventID(v string) Logger {
	lg.EventID = v
	return lg
}

func (lg *MongoLogger) log(level LogLevel, msg string, lf ...LogFields) error {
	log := NewLog(level, msg, lg.Source, lg.Event, lg.EventID, lf...)

	set := bson.M{
		"time":    log.Time,
		"level":   log.Level,
		"message": log.Message,
	}

	if log.Source != "" {
		set["source"] = log.Source
	}

	if log.Event != "" {
		set["event"] = log.Event
	}

	if log.EventID != "" {
		set["event_id"] = log.EventID
	}

	if len(log.Fields) > 0 {
		set["fields"] = log.Fields
	}

	_, err := lg.Coll.InsertOne(context.Background(), set)
	fmt.Print(log.String(lg))
	return err
}

func (lg *MongoLogger) LogExists(filter any) (bool, error) {
	if _, ok := filter.(bson.M); !ok {
		return false, errors.New("filter must have a bson.M type")
	}

	var log Log
	if err := lg.Coll.FindOne(context.Background(), filter).Decode(&log); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}

	return !log.Time.IsZero(), nil
}

func (lg *MongoLogger) Debug(msg string, lf ...LogFields) error {
	return lg.log(DEBUG, msg, lf...)
}

func (lg *MongoLogger) Trace(msg string, lf ...LogFields) error {
	return lg.log(TRACE, msg, lf...)
}

func (lg *MongoLogger) Error(msg string, lf ...LogFields) error {
	return lg.log(ERROR, msg, lf...)
}

func (lg *MongoLogger) Info(msg string, lf ...LogFields) error {
	return lg.log(INFO, msg, lf...)
}

func (lg *MongoLogger) Warn(msg string, lf ...LogFields) error {
	return lg.log(WARN, msg, lf...)
}

func (lg *MongoLogger) Fatal(msg string, lf ...LogFields) error {
	return lg.log(FATAL, msg, lf...)
}

// FindLogs returns logs that match the filter
func (lg *MongoLogger) FindLogs(filter LogFilter, maxLimit int) ([]Log, error) {

	queryFilter := bson.M{}

	// if the from and to fields are not zero, add them to the query filter
	if !filter.From.IsZero() && !filter.To.IsZero() {
		if filter.From.After(filter.To) {
			return nil, errors.New("'from' date cannot be after 'to' date")
		}

		queryFilter["time"] = bson.M{"$gte": filter.From, "$lte": filter.To}
	}

	level := filter.Level
	if level != nil && *level >= TRACE && *level <= FATAL {
		queryFilter["level"] = *level
	}

	if filter.Source != "" {
		queryFilter["source"] = filter.Source
	}

	if filter.Event != "" {
		queryFilter["event"] = filter.Event
	}

	if filter.EventID != "" {
		queryFilter["event_id"] = filter.EventID
	}

	filter.TimeseriesFilter.Limit = int64(maxLimit)

	opts := options.Find().
		SetSort(bson.D{{Key: "time", Value: -1}}). // sort by time field in descending order
		SetLimit(filter.TimeseriesFilter.Limit).
		SetSkip(filter.TimeseriesFilter.Skip)

	var docs []Log
	cursor, err := lg.Coll.Find(context.Background(), queryFilter, opts)
	if err != nil {
		return nil, err
	}

	err = cursor.All(context.Background(), &docs)
	return docs, err
}
