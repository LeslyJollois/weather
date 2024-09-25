package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"golang.org/x/exp/rand"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var (
	ctx      = context.Background()
	logger   *Logger
	db       *sql.DB
	bqClient *bigquery.Client
)

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
}

func main() {
	const numRows = 10000000
	const numLeads = 100000
	const batchSize = 1000
	ctx := context.Background()

	// Define the BigQuery table
	table := bqClient.Dataset(os.Getenv("ENV") + "_weather").Table("lead_event")

	// Helper functions
	randomUUID := func() string {
		return uuid.New().String()
	}

	randomDate := func() string {
		now := time.Now()
		startDate := time.Date(now.Year(), time.January, 1, 0, 0, 0, 0, time.UTC)
		endDate := time.Date(now.Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC).Add(-time.Second)

		randomTime := startDate.Add(time.Duration(rand.Int63n(endDate.Sub(startDate).Nanoseconds())) * time.Nanosecond)

		return randomTime.Format("2006-01-02 15:04:05")
	}

	randomURL := func() string {
		return fmt.Sprintf("https://www.example.com/article-%d.html", rand.Intn(100)+1)
	}

	randomReferrer := func() string {
		if rand.Intn(2) == 0 {
			return ""
		}
		url := randomURL()
		return url
	}

	randomMetas := func() string {
		timeSpent := rand.Intn(100) + 1 // Random time spent between 1 and 100 seconds
		readingRate := rand.Intn(100)   // Random reading rate between 0 et 100 percent
		return fmt.Sprintf(`{"timeSpent": %d, "readingRate": %d}`, timeSpent, readingRate)
	}

	randomConsent := func() bool {
		return rand.Intn(2) == 0
	}

	generateRandomIPv4 := func() string {
		rand.Seed(uint64(time.Now().UnixNano()))

		ip := make([]byte, 4)
		rand.Read(ip)

		ip[0] = byte(rand.Intn(254) + 1)

		return net.IP(net.IPv4(ip[0], ip[1], ip[2], ip[3])).String()
	}

	randomCountryAndCity := func() (string, string) {
		// List of countries and associated cities
		countryCities := map[string][]string{
			"France": {"Paris", "Lyon", "Marseille", "Toulouse"},
			"USA":    {"New York", "Los Angeles", "Chicago", "Houston"},
			"UK":     {"London", "Manchester", "Birmingham", "Edinburgh"},
		}

		// Initialize the seed for random generation
		rand.Seed(uint64(time.Now().UnixNano()))

		// Retrieve a list of available countries
		countries := make([]string, 0, len(countryCities))
		for country := range countryCities {
			countries = append(countries, country)
		}

		// Select a random country
		selectedCountry := countries[rand.Intn(len(countries))]

		// Select a random city from the chosen country
		selectedCity := countryCities[selectedCountry][rand.Intn(len(countryCities[selectedCountry]))]

		return selectedCountry, selectedCity
	}

	// Generate a fixed set of lead UUIDs
	leadUUIDs := make([]string, numLeads)
	for i := 0; i < numLeads; i++ {
		leadUUIDs[i] = randomUUID()
	}

	// Create a slice to hold the rows for BigQuery
	var rows []*bigquery.ValuesSaver

	// Generate data
	for i := 0; i < numRows; i++ {
		brand := "test"
		datetime := randomDate()
		eventUuid := randomUUID()
		leadUUID := leadUUIDs[rand.Intn(numLeads)]
		name := "page_view"
		pageType := "article"
		pageLanguage := "fr_FR"
		device := "desktop"
		url := randomURL()
		referrer := randomReferrer()
		referrerType := "direct"
		relevantReferrer := randomReferrer()
		metas := randomMetas()
		consent := randomConsent()
		country, city := randomCountryAndCity()

		ip := ""
		if consent {
			ip = generateRandomIPv4()
		}

		// Prepare the row for BigQuery
		row := &bigquery.ValuesSaver{
			Schema: bigquery.Schema{
				{Name: "brand", Type: bigquery.StringFieldType},
				{Name: "datetime", Type: bigquery.TimestampFieldType},
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
				brand, datetime, eventUuid, leadUUID, name, pageType, pageLanguage, device, url, referrer, referrerType, relevantReferrer, metas, consent, ip, country, city,
			},
		}
		rows = append(rows, row)

		// Batch insert when reaching the batch size
		if len(rows) >= batchSize {
			if err := table.Inserter().Put(ctx, rows); err != nil {
				logger.LogError("Failed to insert rows to BigQuery: %v", err)
				return
			}
			rows = rows[:0] // Reset the slice
		}
	}

	// Insert any remaining rows
	if len(rows) > 0 {
		if err := table.Inserter().Put(ctx, rows); err != nil {
			logger.LogError("Failed to insert remaining rows to BigQuery: %v", err)
			return
		}
	}

	// Respond with success
	logger.LogInfo("Data inserted successfully")
}
