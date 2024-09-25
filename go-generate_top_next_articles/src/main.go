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
	currentHour := calculationDate.Format("2006-01-02 15") + ":00:00"

	type Article struct {
		URL            string  `bigquery:"url"`
		NextUrl        string  `bigquery:"next_url"`
		ViewCount      int     `bigquery:"view_count"`
		AvgReadingRate float64 `bigquery:"avg_reading_rate"`
		AvgTimeSpent   float64 `bigquery:"avg_time_spent"`
	}

	type TopNextArticle struct {
		ViewCount      int     `bigquery:"view_count"`
		AvgReadingRate float64 `bigquery:"avg_reading_rate"`
		AvgTimeSpent   float64 `bigquery:"avg_time_spent"`
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

	// Step 2: Iterate over brands
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

			// Step 3: Delete old articles for this brand
			deleteQuery := `
				DELETE FROM top_next_articles
				WHERE brand = $1
				AND calculation_period < NOW() - INTERVAL '2 DAYS'
			`
			_, err := db.Exec(deleteQuery, brand)
			if err != nil {
				logger.LogError("Failed to delete old articles for brand %s: %v", brand, err)
				return
			}
			logger.LogInfo("Successfully deleted old read articles for brand: %s", brand)

			// Step 4: Execute BigQuery query to retrieve articles
			bqQuery := fmt.Sprintf(`
				WITH ranked_next_urls AS (
					SELECT 
						le.relevant_referrer AS url,
						le.url AS next_url,
						COUNT(*) AS view_count,
						ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.readingRate') AS FLOAT64)), 2) AS avg_reading_rate,
						ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.timeSpent') AS FLOAT64)), 2) AS avg_time_spent,
						ROW_NUMBER() OVER (PARTITION BY le.relevant_referrer ORDER BY COUNT(*) DESC) AS row_num
					FROM 
						%s_weather.lead_event le
					WHERE 
						le.brand = '%s'
						AND le.relevant_referrer != ""
						AND le.url != le.relevant_referrer
						AND le.page_type = 'article'
						AND le.datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
						AND le.datetime < CURRENT_TIMESTAMP()
					GROUP BY 
						le.relevant_referrer, le.url
				)
				SELECT 
					url,
					next_url,
					view_count,
					avg_reading_rate,
					avg_time_spent
				FROM 
					ranked_next_urls
				WHERE 
					row_num <= 10
				ORDER BY 
					url ASC, view_count DESC;
			`, os.Getenv("ENV"), brand)

			// Execute BigQuery query
			query := bqClient.Query(bqQuery)
			it, err := query.Read(ctx)
			if err != nil {
				logger.LogError("Failed to execute BigQuery for brand %s: %v", brand, err)
				return
			}

			// Process and insert results into PostgreSQL
			topNextArticles := make(map[string]map[string]TopNextArticle)

			for {
				var article Article

				err = it.Next(&article)
				if err == iterator.Done {
					break
				}
				if err != nil {
					logger.LogError("Failed to read from BigQuery iterator for brand %s: %v", brand, err)
					return
				}

				if topNextArticles[article.URL] == nil {
					topNextArticles[article.URL] = make(map[string]TopNextArticle)
				}

				topNextArticles[article.URL][article.NextUrl] = TopNextArticle{
					ViewCount:      article.ViewCount,
					AvgReadingRate: article.AvgReadingRate,
					AvgTimeSpent:   article.AvgTimeSpent,
				}
			}

			// Insert the processed data into PostgreSQL
			insertQuery := `
				INSERT INTO top_next_articles (brand, initial_url, next_url, view_count, avg_reading_rate, avg_time_spent, calculation_period)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (brand, initial_url, next_url, calculation_period)
				DO UPDATE SET 
					view_count = top_next_articles.view_count + EXCLUDED.view_count,
					avg_time_spent = (top_next_articles.avg_time_spent + EXCLUDED.avg_time_spent) / (top_next_articles.view_count + EXCLUDED.view_count), 
					avg_reading_rate = (top_next_articles.avg_reading_rate + EXCLUDED.avg_reading_rate) / (top_next_articles.view_count + EXCLUDED.view_count);
			`

			for url, nextURLs := range topNextArticles {
				for nextURL, topNextArticle := range nextURLs {
					// Insert into PostgreSQL
					_, err = db.Exec(insertQuery, brand, url, nextURL, topNextArticle.ViewCount, topNextArticle.AvgReadingRate, topNextArticle.AvgTimeSpent, currentHour)
					if err != nil {
						logger.LogError("Failed to insert top article for brand %s: %v", brand, err)
						return
					}
					logger.LogInfo("Successfully inserted top next article for brand: %s, url: %s, next url: %s", brand, url, nextURL)
				}
			}
		}(brand) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Step 6: Send success response after all goroutines finish
	logger.LogInfo("Top next articles inserted successfully for all brands.")
}
