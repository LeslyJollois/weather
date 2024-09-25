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
	type ArticleMetrics struct {
		URL            string  `bigquery:"url"`
		ViewCount      int64   `bigquery:"view_count"`
		AvgTimeSpent   float64 `bigquery:"avg_time_spent"`
		AvgReadingRate float64 `bigquery:"avg_reading_rate"`
	}

	calculationDate := time.Now()
	currentHour := calculationDate.Format("2006-01-02 15") + ":00:00"

	// Query to get all brands
	brandsQuery := `SELECT name FROM brand`
	brands, err := db.Query(brandsQuery)
	if err != nil {
		logger.LogError("Error querying brands: %v", err)
		return
	}
	defer brands.Close()

	var wg sync.WaitGroup

	// Prepare to insert metrics into the new table
	insertQuery := `
		INSERT INTO article_metrics (brand, url, view_count, avg_time_spent, avg_reading_rate, calculation_period)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (brand, url, calculation_period)
		DO UPDATE SET 
			view_count = article_metrics.view_count + EXCLUDED.view_count,
			avg_time_spent = (article_metrics.avg_time_spent + EXCLUDED.avg_time_spent) / (article_metrics.view_count + EXCLUDED.view_count), 
			avg_reading_rate = (article_metrics.avg_reading_rate + EXCLUDED.avg_reading_rate) / (article_metrics.view_count + EXCLUDED.view_count);
	`

	for brands.Next() {
		var brandName string
		if err := brands.Scan(&brandName); err != nil {
			logger.LogError("Error scanning brand: %v", err)
			return
		}

		wg.Add(1) // Add to the WaitGroup for each brand

		// Launch a goroutine for each brand
		go func(brandName string) {
			defer wg.Done() // Mark the goroutine as done when finished

			// Query to get metrics for articles in the last 10 minutes for the current brand
			metricsQuery := fmt.Sprintf(`
			SELECT 
				url,
				COUNT(*) AS view_count,
				ROUND(AVG(CAST(JSON_VALUE(metas, '$.timeSpent') AS FLOAT64)), 2) AS avg_time_spent,
				ROUND(AVG(CAST(JSON_VALUE(metas, '$.readingRate') AS FLOAT64)), 2) AS avg_reading_rate
			FROM 
				%s_weather.lead_event
			WHERE 
				brand = '%s'
				AND page_type = 'article'
				AND datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
				AND datetime < CURRENT_TIMESTAMP()
			GROUP BY 
				url
		`, os.Getenv("ENV"), brandName)

			// Execute the query
			metricsJob, err := bqClient.Query(metricsQuery).Run(ctx)
			if err != nil {
				logger.LogError("Error querying article metrics for brand %s: %v", brandName, err)
				return
			}

			// Read the results
			it, err := metricsJob.Read(ctx)
			if err != nil {
				logger.LogError("Error reading metrics results for brand %s: %v", brandName, err)
				return
			}

			// Iterate over the results
			for {
				var articleMetrics ArticleMetrics

				err := it.Next(&articleMetrics)
				if err == iterator.Done {
					break
				}
				if err != nil {
					logger.LogError("Error scanning row for brand %s: %v", brandName, err)
					continue // Skip to the next row on error
				}

				// Insert the metrics into the article_metrics table
				_, err = db.Exec(insertQuery, brandName, articleMetrics.URL, articleMetrics.ViewCount, articleMetrics.AvgTimeSpent, articleMetrics.AvgReadingRate, currentHour)
				if err != nil {
					logger.LogError("Error inserting metrics into article_metrics for brand %s: %v", brandName, err)
				}
				logger.LogInfo("Successfully inserted article metrics for brand: %s, url: %s", brandName, articleMetrics.URL)
			}
		}(brandName) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Articles metrics inserted successfully.")
}
