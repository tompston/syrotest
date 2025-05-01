package syro

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	// cursor, err := lg.Coll.Find(context.Background(), queryFilter, opts)
	// if err != nil {
	// 	return nil, err
	// }
	// err = cursor.All(context.Background(), &docs)
	err := mongoGetDocuments(lg.Coll, queryFilter, opts, &docs)

	return docs, err
}

//

/*
// MongoStorage implementation of the Storage interface
type MongoCronStorage struct {
	Options         CronStorageOptions
	cronListColl    *mongo.Collection
	cronHistoryColl *mongo.Collection
}

// NOTE: add optional auto delete index?
func NewMongoCronStorage(cronListColl, cronHistoryColl *mongo.Collection) (*MongoCronStorage, error) {
	if cronListColl == nil || cronHistoryColl == nil {
		return nil, fmt.Errorf("collections cannot be nil")
	}

	// Create indexes for the collections
	if err := mongodb.NewIndexes().
		Add("name").
		Add("status").
		Add("frequency").
		Create(cronListColl); err != nil {
		return nil, err
	}

	// Create indexes for the collections
	if err := mongodb.NewIndexes().
		Add("name").
		Add("initialized_at").
		Add("execution_time").
		Create(cronHistoryColl); err != nil {
		return nil, err
	}

	return &MongoCronStorage{
		cronListColl:    cronListColl,
		cronHistoryColl: cronHistoryColl,
	}, nil
}

func (m *MongoCronStorage) SetOptions(opt CronStorageOptions) CronStorage {
	m.Options = opt
	return m
}

func (m *MongoCronStorage) GetStorageOptions() CronStorageOptions {
	return m.Options
}

// TODO: refactor so that filter is a variadic parameter
func (m *MongoCronStorage) FindCronJobs() ([]CronJob, error) {
	var docs []CronJob
	err := mongodb.mongoGetDocumentsWithTypes(m.cronListColl, bson.M{}, nil, &docs)
	return docs, err
}

// TODO: test this function
func (m *MongoCronStorage) SetJobsToInactive(source string) error {
	filter := bson.M{"source": source}
	update := bson.M{"$set": bson.M{"status": JobStatusInactive}}
	_, err := m.cronListColl.UpdateMany(context.Background(), filter, update)
	return err
}

// RegisterJob upsert the job name in the database based on the source
// and the job name. If the job does not exist, set the created_at
// field to the current time. If the job already exists,
// update the updated_at field to the current time.
func (m *MongoCronStorage) RegisterJob(source, name, freq, descr string, status JobStatus, fnErr error) error {
	filter := bson.M{
		"source": source,
		"name":   name,
	}

	set := bson.M{
		"frequency":   freq,
		"status":      status,
		"description": descr,
		"updated_at":  time.Now().UTC(),
	}

	if fnErr != nil {
		set["exited_with_error"] = true
		set["error"] = fnErr.Error()
	} else {
		set["exited_with_error"] = false
		set["error"] = ""
	}

	if status == JobStatusDone {
		set["finished_at"] = time.Now().UTC()
	}

	_, err := m.cronListColl.UpdateOne(context.Background(), filter, bson.M{
		"$set":         set,
		"$setOnInsert": bson.M{"created_at": time.Now().UTC()},
	}, mongodb.UpsertOpt)

	return err
}

// Register the execution of a job in the database
func (m *MongoCronStorage) RegisterExecution(ex *CronExecLog) error {
	if ex == nil {
		return fmt.Errorf("job execution cannot be nil")
	}

	_, err := m.cronHistoryColl.InsertOne(context.Background(), ex)
	return err
}

// FindExecutions returns a list of executions based on the filter
func (m *MongoCronStorage) FindExecutions(filter CronExecFilter) ([]CronExecLog, error) {
	queryFilter := bson.M{}

	// if the from and to fields are not zero, add them to the query filter
	if !filter.From.IsZero() && !filter.To.IsZero() {
		if filter.From.After(filter.To) {
			return nil, errors.New("from date cannot be after to date")
		}

		queryFilter["time"] = bson.M{"$gte": filter.From, "$lte": filter.To}
	}

	if filter.Source != "" {
		queryFilter["source"] = filter.Source
	}

	if filter.Name != "" {
		queryFilter["name"] = filter.Name
	}

	if filter.ExecutionTime > 0 {
		queryFilter["execution_time"] = bson.M{"$gte": filter.ExecutionTime}
	}

	// limit := filter.TimeseriesFilter.Limit
	limit := 200

	opts := options.Find().
		SetSort(bson.D{{Key: "initialized_at", Value: -1}}).
		SetLimit(int64(limit)).
		SetSkip(filter.TimeseriesFilter.Skip)

	// var docs []CronExecLog
	// cursor, err := m.cronHistoryColl.Find(context.Background(), queryFilter, opts)
	// if err != nil {
	// 	return nil, err
	// }

	// err = cursor.All(context.Background(), &docs)

	var docs []CronExecLog
	err := mongoGetDocuments(m.cronHistoryColl, queryFilter, opts, &docs)
	return docs, err
}
*/

func mongoGetDocuments[T any](coll *mongo.Collection, filter primitive.M, options *options.FindOptions, results *[]T) error {
	ctx := context.Background()
	cur, err := coll.Find(ctx, filter, options)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)

	return cur.All(ctx, results)
}
