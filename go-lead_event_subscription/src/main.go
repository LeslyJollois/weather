package main

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/oschwald/geoip2-golang"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var (
	ctx      = context.Background()
	logger   *Logger
	bqClient *bigquery.Client
	psClient *pubsub.Client
	ipDb     *geoip2.Reader
)

// Structs for storing lead event data
type LeadEventDataPubSub struct {
	Brand            string                 `json:"brand"`
	UUID             string                 `json:"uuid"`
	LeadUUID         string                 `json:"lead_uuid"`
	Name             string                 `json:"name"`
	PageType         string                 `json:"page_type"`
	PageLanguage     string                 `json:"page_language"`
	Device           string                 `json:"device"`
	Url              string                 `json:"url"`
	Referrer         string                 `json:"referrer"`
	ReferrerType     string                 `json:"referrer_type"`
	RelevantReferrer string                 `json:"relevant_referrer"`
	Metas            map[string]interface{} `json:"metas"`
	Consent          bool                   `json:"consent"`
	IP               string                 `json:"ip"`
}

type IPLocation struct {
	Country string
	City    string
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

	// Accumulate the rows to insert
	var rows []*bigquery.ValuesSaver

	// Extract data from the accumulated messages
	for _, msg := range bp.messages {
		var leadEventDataPubSub LeadEventDataPubSub
		if err := json.Unmarshal(msg.Data, &leadEventDataPubSub); err != nil {
			logger.LogError("Error unmarshalling message: %s", err.Error())
			msg.Nack()
			continue
		}

		// Convert metas to JSON
		metasJSONBytes, err := json.Marshal(leadEventDataPubSub.Metas)
		if err != nil {
			logger.LogError("Error marshalling metas: %s", err.Error())
			msg.Nack()
			continue
		}

		logger.LogInfo("Processing lead event of type %s with uuid %s", leadEventDataPubSub.Name, leadEventDataPubSub.UUID)

		var locationCounty, locationCity string
		if leadEventDataPubSub.Name != "page_behavior" && leadEventDataPubSub.IP != "" {
			ipLocation := getIpLocation(leadEventDataPubSub.IP)
			locationCounty = ipLocation.Country
			locationCity = ipLocation.City
		}

		// Create a row to be inserted
		row := &bigquery.ValuesSaver{
			Schema: bigquery.Schema{
				{Name: "datetime", Type: bigquery.TimestampFieldType},
				{Name: "brand", Type: bigquery.StringFieldType},
				{Name: "uuid", Type: bigquery.StringFieldType},
				{Name: "lead_uuid", Type: bigquery.StringFieldType},
				{Name: "name", Type: bigquery.StringFieldType},
				{Name: "page_type", Type: bigquery.StringFieldType},
				{Name: "page_language", Type: bigquery.StringFieldType},
				{Name: "device", Type: bigquery.StringFieldType},
				{Name: "url", Type: bigquery.StringFieldType},
				{Name: "referrer", Type: bigquery.StringFieldType},
				{Name: "referrer_type", Type: bigquery.StringFieldType},
				{Name: "relevant_referrer", Type: bigquery.StringFieldType},
				{Name: "metas", Type: bigquery.JSONFieldType},
				{Name: "consent", Type: bigquery.BooleanFieldType},
				{Name: "ip", Type: bigquery.StringFieldType},
				{Name: "location_country", Type: bigquery.StringFieldType},
				{Name: "location_city", Type: bigquery.StringFieldType},
			},
			Row: []bigquery.Value{
				time.Now().UTC(),
				leadEventDataPubSub.Brand,
				leadEventDataPubSub.UUID,
				leadEventDataPubSub.LeadUUID,
				leadEventDataPubSub.Name,
				leadEventDataPubSub.PageType,
				leadEventDataPubSub.PageLanguage,
				leadEventDataPubSub.Device,
				leadEventDataPubSub.Url,
				leadEventDataPubSub.Referrer,
				leadEventDataPubSub.ReferrerType,
				leadEventDataPubSub.RelevantReferrer,
				string(metasJSONBytes),
				leadEventDataPubSub.Consent,
				leadEventDataPubSub.IP,
				locationCounty,
				locationCity,
			},
		}

		// Add the row to the batch
		rows = append(rows, row)
	}

	// Perform batch insertion into BigQuery
	inserter := bqClient.Dataset(os.Getenv("ENV") + "_weather").Table("lead_event").Inserter()

	if err := inserter.Put(bp.ctx, rows); err != nil {
		logger.LogError("Failed to insert rows: %v", err)
	} else {
		elapsedTime := time.Since(startTime).Milliseconds()

		logger.LogInfo("Successfully inserted %d rows in BigQuery in %dms.", len(rows), elapsedTime)

		for _, msg := range bp.messages {
			msg.Ack() // Acknowledge the message after processing
		}
	}

	// Clear the batch after processing
	bp.messages = bp.messages[:0]
}

func getIpLocation(ipAddress string) IPLocation {
	// Parse the IP address
	ip := net.ParseIP(ipAddress)

	// Get the IP address info
	record, err := ipDb.City(ip)
	if err != nil {
		log.Fatal(err)
	}

	var ipLocation IPLocation
	ipLocation.Country = record.Country.Names["en"]
	ipLocation.City = record.City.Names["en"]

	return ipLocation
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

	// Open the GeoLite2 database
	ipDb, err = geoip2.Open("GeoLite2-City.mmdb")
	if err != nil {
		logger.LogFatal("[SYSTEM] Unable to open the GeoLite2 IP database: %v", err)
	}
	logger.LogInfo("[SYSTEM] Successfully opened the GeoLite2 IP database")
}

func main() {
	// Create a BatchProcessor
	batchProcessor := NewBatchProcessor(ctx, 1000, 10*time.Second)

	// Start the timer in a separate goroutine
	go batchProcessor.StartBatchTimer()

	// Get the subscription
	sub := psClient.Subscription(os.Getenv("ENV") + "-lead_event")

	// Callback function to process messages
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Add messages to the batch processor
		batchProcessor.AddMessage(msg)
	})

	if err != nil {
		logger.LogFatal("Failed to receive messages: %v", err)
	}
}
