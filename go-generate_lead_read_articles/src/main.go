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
	type ReadArticle struct {
		Brand       string    `bigquery:"brand"`
		LeadUUID    string    `bigquery:"lead_uuid"`
		URL         string    `bigquery:"url"`
		FirstReadAt time.Time `bigquery:"first_read_at"`
	}

	// Step 1: Get all brands from PostgreSQL
	brandsQuery := `SELECT DISTINCT name FROM brand`
	brandsRows, err := db.Query(brandsQuery)
	if err != nil {
		logger.LogError("Failed to query brands: %v", err)
		return
	}
	defer brandsRows.Close()

	var wg sync.WaitGroup

	// Prepare the deletion query for old read articles
	deleteQuery := `
		DELETE FROM lead_read_articles
		WHERE brand = $1
		AND url IN (
			SELECT lra.url
			FROM lead_read_articles lra
			JOIN page p ON p.url = lra.url
			WHERE lra.brand = $1 AND p.publication_date < NOW() - INTERVAL '15 DAYS'
		);
    `

	// Prepare the insertion query for lead_read_articles
	insertQuery := `
        INSERT INTO lead_read_articles (brand, lead_uuid, url, first_read_at)
        VALUES ($1, $2, $3, $4) ON CONFLICT (brand, url, lead_uuid) DO NOTHING
    `

	// Iterate over brands
	for brandsRows.Next() {
		var brand string
		if err := brandsRows.Scan(&brand); err != nil {
			logger.LogError("Failed to scan brand: %v", err)
			return
		}

		wg.Add(1) // Add to the WaitGroup for each brand

		// Launch a goroutine for each brand
		go func(brand string) {
			defer wg.Done() // Mark the goroutine as done when finished

			// Step 2: Delete old articles for this brand
			_, err := db.Exec(deleteQuery, brand)
			if err != nil {
				logger.LogError("Failed to delete old articles for brand %s: %v", brand, err)
				return
			}
			logger.LogInfo("Successfully deleted old read articles for brand: %s", brand)

			// Step 3: Execute the BigQuery query to retrieve articles
			bqQuery := fmt.Sprintf(`
            SELECT 
                le.brand, 
                le.lead_uuid, 
                le.url, 
                MIN(le.datetime) AS first_read_at
            FROM 
                %s_weather.lead_event le
            JOIN 
                %s_weather.page p ON p.url = le.url
            WHERE 
                le.brand = '%s'
                AND le.name = 'page_view'
                AND p.type = 'article'
                AND p.publication_date > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 DAY)
                AND le.datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
                AND le.datetime <= CURRENT_TIMESTAMP()
            GROUP BY 
                le.brand, le.lead_uuid, le.url
        `, os.Getenv("ENV"), os.Getenv("ENV"), brand)

			// Execute BigQuery
			query := bqClient.Query(bqQuery)
			it, err := query.Read(ctx)
			if err != nil {
				logger.LogError("Failed to execute BigQuery for brand %s: %v", brand, err)
				return
			}

			// Step 4: Insert data into PostgreSQL
			for {
				var readArticle ReadArticle

				err = it.Next(&readArticle)
				if err == iterator.Done {
					break
				}
				if err != nil {
					logger.LogError("Failed to read from BigQuery iterator for brand %s: %v", brand, err)
					return
				}

				// Insert into PostgreSQL
				_, err = db.Exec(insertQuery, brand, readArticle.LeadUUID, readArticle.URL, readArticle.FirstReadAt)
				if err != nil {
					logger.LogError("Failed to insert articles for brand %s: %v", brand, err)
					return
				}
				logger.LogInfo("Successfully inserted article for brand: %s, url: %s, lead_uuid: %s", brand, readArticle.URL, readArticle.LeadUUID)
			}
		}(brand) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Step 5: Send success response
	logger.LogInfo("New read articles inserted successfully")
}
