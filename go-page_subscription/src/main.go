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
	"github.com/abadojack/whatlanggo"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
	"golang.org/x/text/language"
	"google.golang.org/api/option"
)

var (
	ctx      = context.Background()
	logger   *Logger
	db       *sql.DB
	bqClient *bigquery.Client
	psClient *pubsub.Client
)

// Structs for storing page data
type PageData struct {
	URL              string               `json:"url"`
	Type             string               `json:"type"`
	Language         string               `json:"language"`
	PublicationDate  PublicationDateTime  `json:"publicationDate"`
	ModificationDate *PublicationDateTime `json:"modificationDate"`
	Title            string               `json:"title"`
	Description      string               `json:"description"`
	Content          string               `json:"content"`
	Section          string               `json:"section"`
	SubSection       *string              `json:"subSection"`
	Image            *string              `json:"image"`
	IsPaid           bool                 `json:"isPaid"`
}

// Structs for storing page data
type PageDataPubSub struct {
	DateTime         time.Time  `json:"datetime"`
	Brand            string     `json:"brand"`
	URL              string     `json:"url"`
	Type             string     `json:"type"`
	Language         string     `json:"language"`
	PublicationDate  time.Time  `json:"publication_date"`
	ModificationDate *time.Time `json:"modification_date"`
	Title            string     `json:"title"`
	Description      string     `json:"description"`
	Content          string     `json:"content"`
	Section          string     `json:"section"`
	SubSection       *string    `json:"sub_section"`
	Image            *string    `json:"image"`
	IsPaid           bool       `json:"is_paid"`
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

type PublicationDateTime time.Time

const publicationDataTimeFormat = "2006-01-02T15:04:05Z07:00"

func (ct *PublicationDateTime) UnmarshalJSON(data []byte) error {
	str := string(data)
	if str == "null" {
		*ct = PublicationDateTime(time.Time{})
		return nil
	}

	t, err := time.Parse(`"`+publicationDataTimeFormat+`"`, str)
	if err != nil {
		return err
	}
	*ct = PublicationDateTime(t)
	return nil
}

func (ct PublicationDateTime) Time() time.Time {
	return time.Time(ct)
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
		logger.LogInfo(string(msg.Data))
		var pageDataPubSub PageDataPubSub
		if err := json.Unmarshal(msg.Data, &pageDataPubSub); err != nil {
			logger.LogError("Error unmarshalling message: %s", err.Error())
			msg.Nack()
			continue
		}

		// Parse the page locale
		pageLocaleInfos, err := language.Parse(pageDataPubSub.Language)
		if err != nil {
			logger.LogError("Failed to parse locale '%s': %v", pageDataPubSub.Language, err)
			msg.Nack()
			continue
		}

		pageLanguage, _ := pageLocaleInfos.Base()

		contentInfo := whatlanggo.Detect(pageDataPubSub.Content)
		contentLanguage := contentInfo.Lang.Iso6391()

		if contentLanguage != pageLanguage.String() {
			logger.LogError("Content language and page locale meta doesn't match: %s / %s", contentLanguage, pageLanguage.String())
			msg.Ack() // Ack the message as we don't want to ingest it
			continue
		}

		logger.LogInfo("Processing page for brand '%s' of type '%s' with url '%s'", pageDataPubSub.Brand, pageDataPubSub.Type, pageDataPubSub.URL)

		page, err := getPageFromDB(pageDataPubSub.Brand, pageDataPubSub.URL)
		if err != nil {
			logger.LogError("Failed to get page: %v", err)
			msg.Nack()
			continue
		}

		if page == nil {
			logger.LogInfo("Page is new")

			// Add insert to the transaction
			query := `INSERT INTO page (brand, type, language, url, publication_date, modification_date, title, description, content, section, sub_section, image, is_paid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
			_, err := tx.Exec(query, pageDataPubSub.Brand, pageDataPubSub.Type, pageDataPubSub.Language, pageDataPubSub.URL, pageDataPubSub.PublicationDate, pageDataPubSub.ModificationDate, pageDataPubSub.Title, pageDataPubSub.Description, pageDataPubSub.Content, pageDataPubSub.Section, pageDataPubSub.SubSection, pageDataPubSub.Image, pageDataPubSub.IsPaid)
			if err != nil {
				tx.Rollback()
				logger.LogError("Error inserting into page: ", err)
				msg.Nack()
				continue
			}

			// Create a row to be inserted in BigQuery
			row := &bigquery.ValuesSaver{
				Schema: bigquery.Schema{
					{Name: "datetime", Type: bigquery.StringFieldType},
					{Name: "brand", Type: bigquery.StringFieldType},
					{Name: "url", Type: bigquery.StringFieldType},
					{Name: "type", Type: bigquery.StringFieldType},
					{Name: "language", Type: bigquery.StringFieldType},
					{Name: "publication_date", Type: bigquery.TimestampFieldType},
					{Name: "modification_date", Type: bigquery.TimestampFieldType},
					{Name: "title", Type: bigquery.StringFieldType},
					{Name: "description", Type: bigquery.StringFieldType},
					{Name: "content", Type: bigquery.StringFieldType},
					{Name: "section", Type: bigquery.StringFieldType},
					{Name: "sub_section", Type: bigquery.StringFieldType},
					{Name: "image", Type: bigquery.StringFieldType},
					{Name: "is_paid", Type: bigquery.BooleanFieldType},
				},
				Row: []bigquery.Value{
					pageDataPubSub.DateTime,
					pageDataPubSub.Brand,
					pageDataPubSub.URL,
					pageDataPubSub.Type,
					pageDataPubSub.Language,
					pageDataPubSub.PublicationDate,
					pageDataPubSub.ModificationDate,
					pageDataPubSub.Title,
					pageDataPubSub.Description,
					pageDataPubSub.Content,
					pageDataPubSub.Section,
					pageDataPubSub.SubSection,
					pageDataPubSub.Image,
					pageDataPubSub.IsPaid,
				},
			}

			// Add the row to the batch
			rows = append(rows, row)

			// Add the message to messages to ack queue
			msgsToAck = append(msgsToAck, msg)
		} else {
			var currentModificationDate, newModificationDate string

			if page.ModificationDate != nil {
				currentModificationDate = page.ModificationDate.Time().Format("2006-01-02T15:04:05Z")
			}

			if pageDataPubSub.ModificationDate != nil {
				newModificationDate = pageDataPubSub.ModificationDate.Format("2006-01-02T15:04:05Z")
			}

			if currentModificationDate != newModificationDate {
				logger.LogInfo("Page has changed")

				// Update page in PostgreSQL
				query := `
				UPDATE page
				SET
					modification_date = $1,
					title = $2,
					description = $3,
					content = $4,
					section = $5,
					sub_section = $6,
					image = $7,
					is_paid = $8
				WHERE
					brand = $9 AND url = $10
			`

				_, err = tx.Exec(query,
					pageDataPubSub.ModificationDate,
					pageDataPubSub.Title,
					pageDataPubSub.Description,
					pageDataPubSub.Content,
					pageDataPubSub.Section,
					pageDataPubSub.SubSection,
					pageDataPubSub.Image,
					pageDataPubSub.IsPaid,
					pageDataPubSub.Brand,
					pageDataPubSub.URL,
				)
				if err != nil {
					logger.LogError("Error updating page: ", err)
					msg.Nack()
					continue
				}

				// Create a row to be inserted in BigQuery
				row := &bigquery.ValuesSaver{
					Schema: bigquery.Schema{
						{Name: "datetime", Type: bigquery.StringFieldType},
						{Name: "brand", Type: bigquery.StringFieldType},
						{Name: "url", Type: bigquery.StringFieldType},
						{Name: "type", Type: bigquery.StringFieldType},
						{Name: "language", Type: bigquery.StringFieldType},
						{Name: "publication_date", Type: bigquery.TimestampFieldType},
						{Name: "modification_date", Type: bigquery.TimestampFieldType},
						{Name: "title", Type: bigquery.StringFieldType},
						{Name: "description", Type: bigquery.StringFieldType},
						{Name: "content", Type: bigquery.StringFieldType},
						{Name: "section", Type: bigquery.StringFieldType},
						{Name: "sub_section", Type: bigquery.StringFieldType},
						{Name: "image", Type: bigquery.StringFieldType},
						{Name: "is_paid", Type: bigquery.BooleanFieldType},
					},
					Row: []bigquery.Value{
						pageDataPubSub.DateTime,
						pageDataPubSub.Brand,
						pageDataPubSub.URL,
						pageDataPubSub.Type,
						pageDataPubSub.Language,
						pageDataPubSub.PublicationDate,
						pageDataPubSub.ModificationDate,
						pageDataPubSub.Title,
						pageDataPubSub.Description,
						pageDataPubSub.Content,
						pageDataPubSub.Section,
						pageDataPubSub.SubSection,
						pageDataPubSub.Image,
						pageDataPubSub.IsPaid,
					},
				}

				// Add the row to the batch
				rows = append(rows, row)

				// Add the message to messages to ack queue
				msgsToAck = append(msgsToAck, msg)
			} else {
				logger.LogInfo("Page has not changed")
			}
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
	inserter := bqClient.Dataset(os.Getenv("ENV") + "_weather").Table("page").Inserter()

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

// getPageFromDB retrieves details of the page from the database
func getPageFromDB(brandName string, url string) (*PageData, error) {
	var page PageData
	query := `
		SELECT
			url,
			type,
			language,
			publication_date,
			modification_date,
			title,
			description,
			content,
			section,
			sub_section,
			image,
			is_paid
		FROM page
		WHERE brand = $1 AND url = $2
	`
	err := db.QueryRow(query, brandName, url).Scan(
		&page.URL,
		&page.Type,
		&page.Language,
		&page.PublicationDate,
		&page.ModificationDate,
		&page.Title,
		&page.Description,
		&page.Content,
		&page.Section,
		&page.SubSection,
		&page.Image,
		&page.IsPaid,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Page does not exist
		}
		return nil, err // Error occurred
	}
	return &page, nil
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
	sub := psClient.Subscription(os.Getenv("ENV") + "-page")

	// Callback function to process messages
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Add messages to the batch processor
		batchProcessor.AddMessage(msg)
	})

	if err != nil {
		logger.LogFatal("Failed to receive messages: %v", err)
	}
}
