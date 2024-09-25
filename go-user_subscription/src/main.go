package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var (
	ctx      = context.Background()
	logger   *Logger
	db       *sql.DB
	bqClient *bigquery.Client
	psClient *pubsub.Client
)

// Structs for storing user data
type UserData struct {
	LeadUUID     string `json:"leadUuid"`
	UserID       string `json:"userID"`
	Email        string `json:"email"`
	FirstName    string `json:"firstName"`
	LastName     string `json:"lastName"`
	IsSubscriber bool   `json:"isSubscriber"`
}

// Structs for storing user data
type UserDataPubSub struct {
	DateTime     time.Time `json:"datetime"`
	Brand        string    `json:"brand"`
	LeadUUID     string    `json:"lead_uuid"`
	UserID       string    `json:"user_id"`
	Email        string    `json:"email"`
	FirstName    string    `json:"first_name"`
	LastName     string    `json:"last_name"`
	IsSubscriber bool      `json:"is_subscriber"`
}

// Logger struct to encapsulate the standard logger
type Logger struct {
	logger *log.Logger
}

// LogInfo writes an informational message
func (l *Logger) LogInfo(format string, args ...interface{}) {
	l.logger.Printf("[INFO] "+format, args...)
}

// LogWarn writes a warning message
func (l *Logger) LogWarn(format string, args ...interface{}) {
	l.logger.Printf("[WARN] "+format, args...)
}

// LogError writes an error message
func (l *Logger) LogError(format string, args ...interface{}) {
	l.logger.Printf("[ERROR] "+format, args...)
}

// LogFatal writes an error message and then exits the application
func (l *Logger) LogFatal(format string, args ...interface{}) {
	l.logger.Fatalf("[FATAL] "+format, args...)
}

// BatchProcessor structure for managing the batch process
type BatchProcessor struct {
	messages     []*pubsub.Message
	batchMutex   sync.Mutex
	batchTimer   *time.Timer
	maxBatchSize int
	maxWaitTime  time.Duration
	ctx          context.Context
}

func NewBatchProcessor(ctx context.Context, maxBatchSize int, maxWaitTime time.Duration) *BatchProcessor {
	return &BatchProcessor{
		messages:     make([]*pubsub.Message, 0, maxBatchSize),
		batchTimer:   time.NewTimer(maxWaitTime),
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		ctx:          ctx,
	}
}

func (bp *BatchProcessor) AddMessage(msg *pubsub.Message) {
	bp.batchMutex.Lock()
	defer bp.batchMutex.Unlock()

	bp.messages = append(bp.messages, msg)

	if len(bp.messages) >= bp.maxBatchSize {
		// Process the batch if the size threshold is reached
		bp.processBatch()
	}
}

func (bp *BatchProcessor) StartBatchTimer() {
	for {
		select {
		case <-bp.batchTimer.C:
			// Process the batch if the time threshold is reached
			bp.batchMutex.Lock()
			if len(bp.messages) > 0 {
				bp.processBatch()
			}
			bp.batchMutex.Unlock()

			// Reset the timer for the next batch
			bp.batchTimer.Reset(bp.maxWaitTime)
		}
	}
}

func (bp *BatchProcessor) processBatch() {
	if len(bp.messages) == 0 {
		return
	}

	logger.LogInfo("Processing %d messages", len(bp.messages))

	startTime := time.Now()

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		logger.LogError("Error starting transaction: ", err)
		return
	}

	// Messages to ack
	var msgsToAck []*pubsub.Message

	// Accumulate the rows to insert
	var rows []*bigquery.ValuesSaver

	// Extract data from the accumulated messages
	for _, msg := range bp.messages {
		var userDataPubSub UserDataPubSub
		if err := json.Unmarshal(msg.Data, &userDataPubSub); err != nil {
			logger.LogError("Error unmarshalling message: %s", err.Error())
			msg.Nack()
			continue
		}

		logger.LogInfo("Processing user for brand '%s' with lead UUID '%s'", userDataPubSub.Brand, userDataPubSub.LeadUUID)

		user, err := getUserFromDB(userDataPubSub.Brand, userDataPubSub.LeadUUID)
		if err != nil {
			logger.LogError("Failed to get user: %v", err)
			msg.Nack()
			continue
		}

		if user == nil {
			logger.LogInfo("User is new")

			// Add insert to the transaction
			query := `INSERT INTO "user" (brand, lead_uuid, user_id, email, first_name, last_name, is_subscriber) VALUES ($1, $2, $3, $4, $5, $6, $7)`
			_, err := tx.Exec(query, userDataPubSub.Brand, userDataPubSub.LeadUUID, userDataPubSub.UserID, userDataPubSub.Email, userDataPubSub.FirstName, userDataPubSub.LastName, userDataPubSub.IsSubscriber)
			if err != nil {
				tx.Rollback()
				logger.LogError("Error inserting into user: ", err)
				msg.Nack()
				continue
			}

			// Create a row to be inserted in BigQuery
			row := &bigquery.ValuesSaver{
				Schema: bigquery.Schema{
					{Name: "datetime", Type: bigquery.StringFieldType},
					{Name: "brand", Type: bigquery.StringFieldType},
					{Name: "lead_uuid", Type: bigquery.StringFieldType},
					{Name: "user_id", Type: bigquery.StringFieldType},
					{Name: "email", Type: bigquery.StringFieldType},
					{Name: "first_name", Type: bigquery.StringFieldType},
					{Name: "last_name", Type: bigquery.StringFieldType},
					{Name: "is_subscriber", Type: bigquery.BooleanFieldType},
				},
				Row: []bigquery.Value{
					userDataPubSub.DateTime,
					userDataPubSub.Brand,
					userDataPubSub.LeadUUID,
					userDataPubSub.UserID,
					userDataPubSub.Email,
					userDataPubSub.FirstName,
					userDataPubSub.LastName,
					userDataPubSub.IsSubscriber,
				},
			}

			// Add the row to the batch
			rows = append(rows, row)

			// Add the message to messages to ack queue
			msgsToAck = append(msgsToAck, msg)
		} else if user.IsSubscriber != userDataPubSub.IsSubscriber {
			logger.LogInfo("User has changed")

			// Update user in PostgreSQL
			query := `UPDATE "user" SET user_id = $1, email = $2, first_name = $3, last_name = $4, is_subscriber = $5 WHERE brand = $6 AND lead_uuid = $7`
			_, err := db.Exec(query, userDataPubSub.UserID, userDataPubSub.Email, userDataPubSub.FirstName, userDataPubSub.LastName, userDataPubSub.IsSubscriber, userDataPubSub.Brand, userDataPubSub.LeadUUID)
			if err != nil {
				logger.LogError("Error updating user: ", err)
				msg.Nack()
				continue
			}

			// Create a row to be inserted in BigQuery
			row := &bigquery.ValuesSaver{
				Schema: bigquery.Schema{
					{Name: "datetime", Type: bigquery.StringFieldType},
					{Name: "brand", Type: bigquery.StringFieldType},
					{Name: "lead_uuid", Type: bigquery.StringFieldType},
					{Name: "user_id", Type: bigquery.StringFieldType},
					{Name: "email", Type: bigquery.StringFieldType},
					{Name: "first_name", Type: bigquery.StringFieldType},
					{Name: "last_name", Type: bigquery.StringFieldType},
					{Name: "is_subscriber", Type: bigquery.BooleanFieldType},
				},
				Row: []bigquery.Value{
					userDataPubSub.DateTime,
					userDataPubSub.Brand,
					userDataPubSub.LeadUUID,
					userDataPubSub.UserID,
					userDataPubSub.Email,
					userDataPubSub.FirstName,
					userDataPubSub.LastName,
					userDataPubSub.IsSubscriber,
				},
			}

			// Add the row to the batch
			rows = append(rows, row)

			// Add the message to messages to ack queue
			msgsToAck = append(msgsToAck, msg)
		} else {
			logger.LogInfo("User has not changed")
		}
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		logger.LogError("Error committing transaction: ", err)
	} else {
		logger.LogInfo("Successfully inserted and updated rows in PostgreSQL.")
	}

	// Perform batch insertion into BigQuery
	inserter := bqClient.Dataset(os.Getenv("ENV") + "_weather").Table("user").Inserter()

	if err := inserter.Put(bp.ctx, rows); err != nil {
		logger.LogError("Failed to insert rows: %v", err)
	} else {
		for _, msg := range msgsToAck {
			msg.Ack() // Acknowledge the message after processing
		}

		logger.LogInfo("Successfully inserted %d rows in BigQuery.", len(rows))
	}

	elapsedTime := time.Since(startTime).Milliseconds()

	logger.LogInfo("Successfully processed %d out of %d messages in %dms.", len(msgsToAck), len(bp.messages), elapsedTime)

	// Clear the batch after processing
	bp.messages = bp.messages[:0]
}

// Get user info from the database
func getUserFromDB(brandName string, leadUuid string) (*UserData, error) {
	var userData UserData
	query := `SELECT user_id, email, first_name, last_name, is_subscriber FROM "user" WHERE brand = $1 AND lead_uuid = $2`
	err := db.QueryRow(query, brandName, leadUuid).Scan(&userData.UserID, &userData.Email, &userData.FirstName, &userData.LastName, &userData.IsSubscriber)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &userData, nil
}

// Initialize Redis and SQL clients
func init() {
	// Init logger
	logger = &Logger{
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}

	var err error

	// Load environment variables from .env file
	if err = godotenv.Load(); err != nil {
		logger.LogFatal("[SYSTEM] Error loading .env file")
	}

	db, err = sql.Open("postgres", os.Getenv("POSTGRES_DSN"))
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to connect to PostgreSQL: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to PostgreSQL")

	bqClient, err = bigquery.NewClient(ctx, os.Getenv("GCP_PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GCP_CREDENTIALS_FILE")))
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to connect to BigQuery: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to BigQuery")

	psClient, err = pubsub.NewClient(ctx, os.Getenv("GCP_PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GCP_CREDENTIALS_FILE")))
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to create Pub/Sub client: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to PubSub")
}

func main() {
	// Create a BatchProcessor
	batchProcessor := NewBatchProcessor(ctx, 10, 10*time.Second)

	// Start the timer in a separate goroutine
	go batchProcessor.StartBatchTimer()

	// Get the subscription
	sub := psClient.Subscription(os.Getenv("ENV") + "-user")

	// Callback function to process messages
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Add messages to the batch processor
		batchProcessor.AddMessage(msg)
	})

	if err != nil {
		logger.LogFatal("Failed to receive messages: %v", err)
	}
}
