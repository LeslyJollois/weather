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
	// Structure to store the results from BigQuery
	type ViewCount struct {
		LeadUUID       string  `bigquery:"lead_uuid"`
		ViewCount      int     `bigquery:"view_count"`
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

			// Iterate over 24-hour intervals
			for t := startDate; t.Before(endDate); t = t.Add(24 * time.Hour) {
				intervalStart := t
				intervalEnd := t.Add(24 * time.Hour)
				currentDay := intervalStart.Format("2006-01-02 ") + "00:00:00"

				// Query BigQuery to get lead engagement metrics for the current interval
				intervalQuery := fmt.Sprintf(`
				WITH leads AS (
					SELECT 
						lead_uuid,
					FROM 
						%s_weather.lead_event
					WHERE 
						brand = @brand
						AND datetime >= TIMESTAMP_SUB(@intervalStart, INTERVAL 90 DAY)
						AND datetime < @intervalStart
					GROUP BY 
						lead_uuid
					HAVING COUNT(*) >= 10
				)
				SELECT 
					le.lead_uuid,
					COUNT(*) AS view_count,
					ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.timeSpent') AS FLOAT64)), 2) AS avg_time_spent,
					ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.readingRate') AS FLOAT64)), 2) AS avg_reading_rate
				FROM 
					%s_weather.lead_event le
				LEFT JOIN 
					leads l ON l.lead_uuid = le.lead_uuid AND l.brand = @brand
				WHERE 
					le.brand = @brand
					AND l.lead_uuid IS NOT NULL
					AND le.datetime >= @intervalStart AND le.datetime < @intervalEnd
				GROUP BY 
					le.lead_uuid
			`, os.Getenv("ENV"), os.Getenv("ENV"))

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

				// Prepare to insert lead engagement metrics into the PostgreSQL table
				insertQuery := `
				INSERT INTO lead_engagement_metrics (
					brand,
					lead_uuid,
					view_count,
					avg_time_spent,
					avg_reading_rate,
					calculation_period
				) VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (brand, lead_uuid, calculation_period)
				DO UPDATE SET
					view_count = lead_engagement_metrics.view_count + EXCLUDED.view_count, 
					avg_time_spent = (lead_engagement_metrics.avg_time_spent + EXCLUDED.avg_time_spent) / (lead_engagement_metrics.view_count + EXCLUDED.view_count), 
					avg_reading_rate = (lead_engagement_metrics.avg_reading_rate + EXCLUDED.avg_reading_rate) / (lead_engagement_metrics.view_count + EXCLUDED.view_count);
			`

				// Iterate over rows
				for {
					var v ViewCount

					// Get the next row
					err := rowIterator.Next(&v)
					if err == iterator.Done {
						break // End of results
					}
					if err != nil {
						logger.LogError("Error iterating over rows for brand %s: %v", brandName, err)
						break
					}

					// Insert to PostgreSQL
					_, err = db.Exec(insertQuery, brandName, v.LeadUUID, v.ViewCount, v.AvgTimeSpent, v.AvgReadingRate, currentDay)
					if err != nil {
						logger.LogError("Error inserting lead engagment metrics for brand %s: %v", brandName, err)
					}
					logger.LogInfo("Successfully inserted lead engagment metrics for brand: %s, leadUuid: %s, intervalStart: %s, intervalEnd: %s", brandName, v.LeadUUID, intervalStart, intervalEnd)
				}
			}
		}(brandName) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Historical lead engagment metrics generated successfully.")
}
