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

	// Parse query parameters
	startDateStr := os.Getenv("START_DATE")
	endDateStr := os.Getenv("END_DATE")

	if startDateStr == "" || endDateStr == "" {
		logger.LogError("Missing start_date or end_date")
		return
	}

	startDate, err := time.Parse(time.RFC3339, startDateStr)
	if err != nil {
		logger.LogError("Invalid start_date format")
		return
	}

	endDate, err := time.Parse(time.RFC3339, endDateStr)
	if err != nil {
		logger.LogError("Invalid end_date format")
		return
	}

	// Query to get all brands
	brandsQuery := `SELECT name FROM brand`
	brands, err := db.Query(brandsQuery)
	if err != nil {
		logger.LogError("Error querying brands: %v", err)
		return
	}
	defer brands.Close()

	var wg sync.WaitGroup

	// Loop through each brand
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

			// Iterate over 1-hour intervals
			for t := startDate; t.Before(endDate); t = t.Add(1 * time.Hour) {
				intervalStart := t
				intervalEnd := t.Add(1 * time.Hour)
				currentHour := intervalStart.Format("2006-01-02 15") + ":00:00"

				// Query BigQuery to get article metrics for the current interval
				intervalQuery := fmt.Sprintf(`
				SELECT 
					url,
					COUNT(*) AS view_count,
					ROUND(AVG(CAST(JSON_VALUE(metas, '$.timeSpent') AS FLOAT64)), 2) AS avg_time_spent,
					ROUND(AVG(CAST(JSON_VALUE(metas, '$.readingRate') AS FLOAT64)), 2) AS avg_reading_rate
				FROM 
					%s_weather.lead_event
				WHERE 
					brand = @brand
					AND datetime >= @intervalStart AND datetime < @intervalEnd
				GROUP BY 
					brand, url;
			`, os.Getenv("ENV"))

				// Execute the BigQuery query
				query := bqClient.Query(intervalQuery)
				query.Parameters = []bigquery.QueryParameter{
					{Name: "brand", Value: brandName},
					{Name: "intervalStart", Value: intervalStart},
					{Name: "intervalEnd", Value: intervalEnd},
				}

				it, err := query.Run(ctx)
				if err != nil {
					logger.LogError("Error running query for brand %s: %v", brandName, err)
					continue
				}

				// Fetch results
				rowIterator, err := it.Read(ctx)
				if err != nil {
					logger.LogError("Error reading results for brand %s: %v", brandName, err)
					continue
				}

				// Prepare to insert metrics into the PostgreSQL table
				insertQuery := `
				INSERT INTO article_metrics (brand, url, view_count, avg_time_spent, avg_reading_rate, calculation_period)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (brand, url, calculation_period)
				DO UPDATE SET 
					view_count = article_metrics.view_count + EXCLUDED.view_count,
					avg_time_spent = (article_metrics.avg_time_spent + EXCLUDED.avg_time_spent) / (article_metrics.view_count + EXCLUDED.view_count), 
					avg_reading_rate = (article_metrics.avg_reading_rate + EXCLUDED.avg_reading_rate) / (article_metrics.view_count + EXCLUDED.view_count);
			`

				// Iterate over rows
				for {
					var articleMetrics ArticleMetrics

					// Get the next row
					err := rowIterator.Next(&articleMetrics)
					if err == iterator.Done {
						break // End of results
					}
					if err != nil {
						logger.LogError("Error iterating over rows for brand %s: %v", brandName, err)
						break
					}

					// Insert to PostgreSQL
					_, err = db.Exec(insertQuery, brandName, articleMetrics.URL, articleMetrics.ViewCount, articleMetrics.AvgTimeSpent, articleMetrics.AvgReadingRate, currentHour)
					if err != nil {
						logger.LogError("Error inserting metrics into article_metrics for brand %s: %v", brandName, err)
					}
					logger.LogInfo("Successfully inserted article metrics for brand: %s, url: %s, intervalStart: %s, intervalEnd: %s", brandName, articleMetrics.URL, intervalStart, intervalEnd)
				}
			}
		}(brandName) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Historical article metrics generated successfully.")
}
