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
	calculationDate := time.Now()
	currentDay := calculationDate.Format("2006-01-02 ") + "00:00:00"

	// Structure to store the results from BigQuery
	type ViewCount struct {
		LeadUUID       string  `bigquery:"lead_uuid"`
		ViewCount      int     `bigquery:"view_count"`
		AvgTimeSpent   float64 `bigquery:"avg_time_spent"`
		AvgReadingRate float64 `bigquery:"avg_reading_rate"`
	}

	// Step 1: Retrieve unique brands from PostgreSQL
	brandsQuery := `SELECT name, page_view_threshold FROM brand`
	rows, err := db.Query(brandsQuery)
	if err != nil {
		logger.LogError("Failed to retrieve brands: %v", err)
		return
	}
	defer rows.Close()

	var wg sync.WaitGroup

	// Step 3: Iterate over the brands
	for rows.Next() {
		var brand string
		var pageViewThreshold int
		if err := rows.Scan(&brand, &pageViewThreshold); err != nil {
			logger.LogError("Failed to scan brand: %v", err)
			return
		}

		wg.Add(1) // Add to the WaitGroup for each brand

		// Launch a goroutine for each brand
		go func(brand string, pageViewThreshold int) {
			defer wg.Done() // Mark the goroutine as done when finished

			// Step 3: Delete old data for the current brand
			deleteOldDataQuery := `
				DELETE FROM 
					lead_engagement_metrics
				WHERE 
					brand = $1
					AND calculation_period < NOW() - INTERVAL '90 DAY'
			`
			_, err = db.Exec(deleteOldDataQuery, brand)
			if err != nil {
				logger.LogError("Failed to delete old data for brand %s: %v", brand, err)
				return
			}

			// Step 4: Define the BigQuery query for the current brand
			query := fmt.Sprintf(`
				WITH leads AS (
					SELECT 
						lead_uuid,
					FROM 
						%s_weather.lead_event
					WHERE 
						brand = '%s'
						AND datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
						AND datetime < CURRENT_TIMESTAMP()
					GROUP BY 
						lead_uuid
					HAVING COUNT(*) >= %d
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
					le.brand = '%s'
					AND l.lead_uuid IS NOT NULL
					AND le.datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
					AND le.datetime < CURRENT_TIMESTAMP()
				GROUP BY 
					le.lead_uuid
			`, os.Getenv("ENV"), brand, pageViewThreshold, os.Getenv("ENV"), brand)

			// Run BigQuery query
			q := bqClient.Query(query)
			it, err := q.Read(ctx)
			if err != nil {
				logger.LogError("Failed to execute BigQuery for brand %s: %v", brand, err)
				return
			}

			// Step 5: Process the results from BigQuery and insert into PostgreSQL
			var viewCounts []ViewCount
			for {
				var v ViewCount
				err := it.Next(&v)
				if err == iterator.Done {
					break
				}
				if err != nil {
					logger.LogError("Failed to read BigQuery results for brand %s: %v", brand, err)
					return
				}
				viewCounts = append(viewCounts, v)
			}

			// Insert into PostgreSQL
			for _, v := range viewCounts {
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
				_, err := db.Exec(insertQuery, brand, v.LeadUUID, v.ViewCount, v.AvgTimeSpent, v.AvgReadingRate, currentDay)
				if err != nil {
					logger.LogError("Failed to insert data into PostgreSQL for brand %s : %v", brand, err)
					return
				}
				logger.LogInfo("Successfully inserted lead engagement metrics for brand: %s, leadUuid: %s", brand, v.LeadUUID)
			}
		}(brand, pageViewThreshold) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Send response
	logger.LogInfo("Lead engagement metrics calculated and stored successfully for all brands")
}
