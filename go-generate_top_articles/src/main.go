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
		Brand          string              `bigquery:"brand"`
		URL            string              `bigquery:"url"`
		Section        string              `bigquery:"section"`
		SubSection     bigquery.NullString `bigquery:"sub_section"`
		ViewCount      int                 `bigquery:"view_count"`
		AvgReadingRate float64             `bigquery:"avg_reading_rate"`
		AvgTimeSpent   float64             `bigquery:"avg_time_spent"`
		RecencyWeight  float64             `bigquery:"recency_weight"`
	}

	type TopArticle struct {
		ViewCount      int     `bigquery:"view_count"`
		AvgReadingRate float64 `bigquery:"avg_reading_rate"`
		AvgTimeSpent   float64 `bigquery:"avg_time_spent"`
		RecencyWeight  float64 `bigquery:"recency_weight"`
	}

	// Step 1: Get all brands from PostgreSQL
	brandsQuery := `SELECT DISTINCT name FROM brand`
	brandsRows, err := db.Query(brandsQuery)
	if err != nil {
		logger.LogError("Failed to query brands: %s", err)
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

			// Step 3: Delete old top articles for this brand
			deleteQuery := `
				DELETE FROM top_articles
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
				SELECT
					p.brand,
					p.url,
					p.section,
					p.sub_section,
					COUNT(*) AS view_count,
					ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.readingRate') AS FLOAT64)), 2) AS avg_reading_rate,
					ROUND(AVG(CAST(JSON_VALUE(le.metas, '$.timeSpent') AS FLOAT64)), 2) AS avg_time_spent,
					ROUND(SUM(IF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), le.datetime, SECOND) > 0, 1 / (TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), le.datetime, SECOND) / 3600), 0))) AS recency_weight
				FROM
					%s_weather.lead_event le
				JOIN
					%s_weather.page p ON p.url = le.url
				WHERE
					le.datetime >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
					AND le.datetime < CURRENT_TIMESTAMP()
					AND p.brand = '%s'
				GROUP BY
					p.brand, p.url, p.section, p.sub_section
				ORDER BY
					recency_weight DESC
			`, os.Getenv("ENV"), os.Getenv("ENV"), brand)

			// Execute BigQuery query
			query := bqClient.Query(bqQuery)
			it, err := query.Read(ctx)
			if err != nil {
				logger.LogError("Failed to execute BigQuery for brand %s: %v", brand, err)
				return
			}

			// Process and insert results into PostgreSQL
			topArticles := make(map[string]TopArticle)
			topArticlesSection := make(map[string]map[string]TopArticle)
			topArticlesSectionSubSection := make(map[string]map[string]map[string]TopArticle)

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

				topArticles[article.URL] = TopArticle{
					ViewCount:      article.ViewCount,
					AvgReadingRate: article.AvgReadingRate,
					AvgTimeSpent:   article.AvgTimeSpent,
					RecencyWeight:  article.RecencyWeight,
				}

				if topArticlesSection[article.URL] == nil {
					topArticlesSection[article.URL] = make(map[string]TopArticle)
				}

				topArticlesSection[article.URL][article.Section] = TopArticle{
					ViewCount:      article.ViewCount,
					AvgReadingRate: article.AvgReadingRate,
					AvgTimeSpent:   article.AvgTimeSpent,
					RecencyWeight:  article.RecencyWeight,
				}

				if article.SubSection.Valid && article.SubSection.String() != "" {
					if topArticlesSectionSubSection[article.URL] == nil {
						topArticlesSectionSubSection[article.URL] = make(map[string]map[string]TopArticle)
					}

					if topArticlesSectionSubSection[article.URL][article.Section] == nil {
						topArticlesSectionSubSection[article.URL][article.Section] = make(map[string]TopArticle)
					}

					topArticlesSectionSubSection[article.URL][article.Section][article.SubSection.String()] = TopArticle{
						ViewCount:      article.ViewCount,
						AvgReadingRate: article.AvgReadingRate,
						AvgTimeSpent:   article.AvgTimeSpent,
						RecencyWeight:  article.RecencyWeight,
					}
				}
			}

			// Insert the processed data into PostgreSQL
			insertQuery := `
			    INSERT INTO top_articles (brand, url, view_count, avg_reading_rate, avg_time_spent, recency_weight, section, sub_section, calculation_period)
			    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (brand, url, calculation_period)
				DO UPDATE SET 
					view_count = top_articles.view_count + EXCLUDED.view_count,
					avg_time_spent = (top_articles.avg_time_spent + EXCLUDED.avg_time_spent) / (top_articles.view_count + EXCLUDED.view_count), 
					avg_reading_rate = (top_articles.avg_reading_rate + EXCLUDED.avg_reading_rate) / (top_articles.view_count + EXCLUDED.view_count),
					recency_weight = (top_articles.recency_weight + EXCLUDED.recency_weight) / (top_articles.view_count + EXCLUDED.view_count);
			`

			for url, topArticle := range topArticles {
				// Insert into PostgreSQL
				_, err = db.Exec(insertQuery, brand, url, topArticle.ViewCount, topArticle.AvgReadingRate, topArticle.AvgTimeSpent, topArticle.RecencyWeight, nil, nil, currentHour)
				if err != nil {
					logger.LogError("Failed to insert top article for brand %s: %v", brand, err)
					return
				}
				logger.LogInfo("Successfully inserted top article for brand: %s, url: %s", brand, url)
			}

			for url, sections := range topArticlesSection {
				for section, topArticle := range sections {
					// Insert into PostgreSQL
					_, err = db.Exec(insertQuery, brand, url, topArticle.ViewCount, topArticle.AvgReadingRate, topArticle.AvgTimeSpent, topArticle.RecencyWeight, section, nil, currentHour)
					if err != nil {
						logger.LogError("Failed to insert top article for brand %s and section %s: %v", brand, err, section)
						return
					}
					logger.LogInfo("Successfully inserted top article for brand: %s, url: %s, section: %s", brand, url, section)
				}
			}

			for url, sections := range topArticlesSectionSubSection {
				for section, subSections := range sections {
					for subSection, topArticle := range subSections {
						// Insert into PostgreSQL
						_, err = db.Exec(insertQuery, brand, url, topArticle.ViewCount, topArticle.AvgReadingRate, topArticle.AvgTimeSpent, topArticle.RecencyWeight, section, subSection, currentHour)
						if err != nil {
							logger.LogError("Failed to insert top article for brand %s, section %s and sub section %s: %v", brand, err, section, subSection)
							return
						}
						logger.LogInfo("Successfully inserted top article for brand: %s, url: %s, section: %s, sub section: %s", brand, url, section, subSection)
					}
				}
			}

		}(brand) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Step 6: Send success response after all goroutines finish
	logger.LogInfo("Top articles inserted successfully for all brands")
}
