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
	currentDate := calculationDate.Format("2006-01-02")

	type ArticleCount struct {
		LeadUUID       string  `bigquery:"lead_uuid"`
		Section        string  `bigquery:"section"`
		ArticleCount   int     `bigquery:"article_count"`
		AvgTimeSpent   float64 `bigquery:"avg_time_spent"`
		AvgReadingRate float64 `bigquery:"avg_reading_rate"`
	}

	// 1. Fetch the brands from the `brand` table in PostgreSQL
	getBrandsQuery := `
		SELECT name FROM brand
	`
	rows, err := db.Query(getBrandsQuery)
	if err != nil {
		logger.LogError("Failed to fetch brands: %v", err)
		return
	}
	defer rows.Close()

	var wg sync.WaitGroup

	// 2. Prepare the SQL insert statement for PostgreSQL
	insertStmt, err := db.Prepare(`
		INSERT INTO lead_section_article_count (brand, lead_uuid, section, article_count, avg_time_spent, avg_reading_rate, calculation_period)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (brand, lead_uuid, section, calculation_period)
		DO UPDATE SET 
			article_count = lead_section_article_count.article_count + EXCLUDED.article_count, 
			avg_time_spent = (lead_section_article_count.avg_time_spent + EXCLUDED.avg_time_spent) / (lead_section_article_count.article_count + EXCLUDED.article_count), 
			avg_reading_rate = (lead_section_article_count.avg_reading_rate + EXCLUDED.avg_reading_rate) / (lead_section_article_count.article_count + EXCLUDED.article_count);
	`)
	if err != nil {
		logger.LogError("Failed to prepare insert statement: %v", err)
		return
	}
	defer insertStmt.Close()

	// 3. For each brand, query BigQuery and insert the results into PostgreSQL
	for rows.Next() {
		var brand string
		if err := rows.Scan(&brand); err != nil {
			logger.LogError("Failed to scan brand: %v", err)
			continue
		}

		wg.Add(1) // Add to the WaitGroup for each brand

		// Launch a goroutine for each brand
		go func(brand string) {
			defer wg.Done() // Mark the goroutine as done when finished

			// 4. Delete old data for this specific brand in PostgreSQL
			deleteOldDataQuery := `
				DELETE FROM 
					lead_section_article_count
				WHERE 
					brand = $1
					AND calculation_period < NOW() - INTERVAL '1 MONTH'
			`
			_, err := db.Exec(deleteOldDataQuery, brand)
			if err != nil {
				logger.LogError("Failed to delete old data for brand %s: %v", brand, err)
				return
			}
			logger.LogInfo("Successfully deleted old lead section article count for brand: %s", brand)

			// 5. Build and execute the BigQuery query for each brand
			query := fmt.Sprintf(`
				SELECT 
					le.lead_uuid, 
					p.section, 
					COUNT(DISTINCT le.url) AS article_count,
					ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.timeSpent') AS FLOAT64)), 2) AS avg_time_spent,
					ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.readingRate') AS FLOAT64)), 2) AS avg_reading_rate
				FROM 
					%s_weather.lead_event AS le
				LEFT JOIN 
					%s_weather.page AS p ON p.url = le.url AND p.brand = @brand
				WHERE 
					le.brand = @brand
					AND le.datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
					AND le.datetime <= CURRENT_TIMESTAMP()
				GROUP BY 
					le.lead_uuid, p.section, DATE_TRUNC(le.datetime, MONTH)
				HAVING 
					COUNT(*) > 0
			`, os.Getenv("ENV"), os.Getenv("ENV"))

			// Set the BigQuery query to include the brand
			q := bqClient.Query(query)
			q.Parameters = []bigquery.QueryParameter{
				{
					Name:  "brand",
					Value: brand,
				},
			}

			// Execute the BigQuery query
			it, err := q.Read(ctx)
			if err != nil {
				logger.LogError("Failed to execute BigQuery for brand %s: %v", brand, err)
				return
			}

			// 6. Insert the results into PostgreSQL
			for {
				var articleCount ArticleCount
				err := it.Next(&articleCount)
				if err == iterator.Done {
					break
				}
				if err != nil {
					logger.LogError("Error reading BigQuery result for brand %s: %v", brand, err)
					break
				}

				// Execute the insert statement
				_, err = insertStmt.Exec(brand, articleCount.LeadUUID, articleCount.Section, articleCount.ArticleCount, articleCount.AvgTimeSpent, articleCount.AvgReadingRate, currentDate)
				if err != nil {
					logger.LogError("Failed to insert data for brand %s: %v", brand, err)
					break
				}
				logger.LogInfo("Successfully inserted lead section article count for brand: %s, leadUuid: %s, section: %s", brand, articleCount.LeadUUID, articleCount.Section)
			}
		}(brand) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Lead lead section article count inserted successfully")
}
