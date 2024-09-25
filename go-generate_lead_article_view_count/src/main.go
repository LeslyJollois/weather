package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
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
	type LeadViewCount struct {
		LeadUUID  string `bigquery:"lead_uuid"`
		ViewCount int64  `bigquery:"view_count"`
	}

	calculationDate := time.Now()
	currentDate := calculationDate.Format("2006-01-02")

	// Step 1: Fetch the distinct brands from PostgreSQL
	brandsQuery := `
		SELECT name 
		FROM brand
	`
	rows, err := db.Query(brandsQuery)
	if err != nil {
		logger.LogError("Failed to fetch brands from PostgreSQL: %v", err)
		return
	}
	defer rows.Close()

	var wg sync.WaitGroup

	// Step 2: Prepare the PostgreSQL insertion query
	insertQuery := `
		INSERT INTO lead_article_view_count (brand, lead_uuid, view_count, calculation_period)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (brand, lead_uuid, calculation_period)
		DO UPDATE SET view_count = lead_article_view_count.view_count + EXCLUDED.view_count;
	`

	// Step 3: Iterate over the brands
	for rows.Next() {
		var brand string
		if err := rows.Scan(&brand); err != nil {
			logger.LogError("Failed to scan brand: %v", err)
			return
		}

		wg.Add(1) // Add to the WaitGroup for each brand

		// Launch a goroutine for each brand
		go func(brand string) {
			defer wg.Done() // Mark the goroutine as done when finished

			// Step 4: Delete old data for the current brand in PostgreSQL
			deleteOldDataQuery := `
			DELETE FROM
				lead_article_view_count
			WHERE
				brand = $1
				AND calculation_period < NOW() - INTERVAL '3 MONTH'
		`
			if _, err := db.Exec(deleteOldDataQuery, brand); err != nil {
				logger.LogError("Failed to delete old data for brand %s: %v", brand, err)
				return
			}
			logger.LogInfo("Successfully deleted old article view count for brand: %s", brand)

			// Start a transaction
			tx, err := db.Begin()
			if err != nil {
				logger.LogError("Failed to start transaction: %v", err)
				return
			}

			// Step 5: Fetch view count data for the current brand from BigQuery
			brandQuery := fmt.Sprintf(`
			SELECT
				le.lead_uuid,
				COUNT(*) AS view_count
			FROM
				%s_weather.lead_event le
			WHERE
				le.page_type = 'article'
				AND le.brand = @brand
				AND le.datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
				AND le.datetime <= CURRENT_TIMESTAMP()
			GROUP BY
				le.lead_uuid
		`, os.Getenv("ENV"))

			// Prepare the query job
			q := bqClient.Query(brandQuery)
			q.Parameters = []bigquery.QueryParameter{
				{Name: "brand", Value: brand},
			}

			// Run the query and read the results
			it, err := q.Read(context.Background())
			if err != nil {
				_ = tx.Rollback() // Rollback the transaction
				logger.LogError("Failed to run BigQuery: %v", err)
				return
			}

			// Step 6: Create a slice to hold the results
			var results []LeadViewCount

			// Iterate over the results
			for {
				var lvc LeadViewCount
				err := it.Next(&lvc)
				if err == iterator.Done {
					break // No more data
				}
				if err != nil {
					logger.LogError("Error iterating over view count rows for brand %s: %v", brand, err)
					_ = tx.Rollback() // Rollback the transaction
					return
				}
				results = append(results, lvc)
			}

			// Step 7: Insert the data into PostgreSQL for the current brand
			for _, lvc := range results {
				if _, err = tx.Exec(insertQuery, brand, lvc.LeadUUID, lvc.ViewCount, currentDate); err != nil {
					logger.LogError("Failed to insert view count for brand %s into PostgreSQL: %v", brand, err)
					_ = tx.Rollback() // Rollback the transaction
					return
				}
				logger.LogInfo("Successfully inserted article view count for brand: %s, leadUuid: %s", brand, lvc.LeadUUID)
			}

			// Commit the transaction
			if err := tx.Commit(); err != nil {
				logger.LogError("Failed to commit transaction: %v", err)
				return
			}
		}(brand) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Lead article view count inserted successfully")
}
