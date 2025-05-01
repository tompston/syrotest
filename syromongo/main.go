package syromongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/tompston/syro"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoLogger struct {
	Coll     *mongo.Collection
	Settings *syro.LoggerSettings
	Source   string
	Event    string
	EventID  string
}

func NewMongoLogger(coll *mongo.Collection, settings *syro.LoggerSettings) *MongoLogger {
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

func (lg *MongoLogger) GetProps() syro.LoggerProps {
	return syro.LoggerProps{
		Settings: lg.Settings,
		Source:   lg.Source,
		Event:    lg.Event,
		EventID:  lg.EventID,
	}
}

func (lg *MongoLogger) Name() string {
	return "mongo"
}

func (lg *MongoLogger) SetSource(v string) syro.Logger {
	lg.Source = v
	return lg
}

func (lg *MongoLogger) SetEvent(v string) syro.Logger {
	lg.Event = v
	return lg
}

func (lg *MongoLogger) SetEventID(v string) syro.Logger {
	lg.EventID = v
	return lg
}

func (lg *MongoLogger) log(level syro.LogLevel, msg string, lf ...syro.LogFields) error {
	log := syro.NewLog(level, msg, lg.Source, lg.Event, lg.EventID, lf...)
	_, err := lg.Coll.InsertOne(context.Background(), log)
	fmt.Print(log.String(lg))
	return err
}

func (lg *MongoLogger) LogExists(filter any) (bool, error) {
	if _, ok := filter.(bson.M); !ok {
		return false, errors.New("filter must have a bson.M type")
	}

	var log syro.Log
	if err := lg.Coll.FindOne(context.Background(), filter).Decode(&log); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}

	return !log.Time.IsZero(), nil
}

func (lg *MongoLogger) Debug(msg string, lf ...syro.LogFields) error {
	return lg.log(syro.DEBUG, msg, lf...)
}

func (lg *MongoLogger) Trace(msg string, lf ...syro.LogFields) error {
	return lg.log(syro.TRACE, msg, lf...)
}

func (lg *MongoLogger) Error(msg string, lf ...syro.LogFields) error {
	return lg.log(syro.ERROR, msg, lf...)
}

func (lg *MongoLogger) Info(msg string, lf ...syro.LogFields) error {
	return lg.log(syro.INFO, msg, lf...)
}

func (lg *MongoLogger) Warn(msg string, lf ...syro.LogFields) error {
	return lg.log(syro.WARN, msg, lf...)
}

func (lg *MongoLogger) Fatal(msg string, lf ...syro.LogFields) error {
	return lg.log(syro.FATAL, msg, lf...)
}

// FindLogs returns logs that match the filter
func (lg *MongoLogger) FindLogs(filter syro.LogFilter, maxLimit int) ([]syro.Log, error) {

	queryFilter := bson.M{}

	// if the from and to fields are not zero, add them to the query filter
	if !filter.From.IsZero() && !filter.To.IsZero() {
		if filter.From.After(filter.To) {
			return nil, errors.New("'from' date cannot be after 'to' date")
		}

		queryFilter["time"] = bson.M{"$gte": filter.From, "$lte": filter.To}
	}

	level := filter.Level
	if level != nil && *level >= syro.TRACE && *level <= syro.FATAL {
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

	var docs []syro.Log
	cursor, err := lg.Coll.Find(context.Background(), queryFilter, opts)
	if err != nil {
		return nil, err
	}

	err = cursor.All(context.Background(), &docs)
	return docs, err
}
