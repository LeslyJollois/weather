package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"math"
	"os"
	"sync"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
)

var (
	ctx    = context.Background()
	logger *Logger
	db     *sql.DB
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
}

func main() {
	// Retrieve the list of all brands
	rowsBrands, err := db.Query("SELECT name FROM brand")
	if err != nil {
		logger.LogError("Failed to retrieve brands")
		return
	}
	defer rowsBrands.Close()

	var wg sync.WaitGroup

	// Function to calculate cosine similarity
	cosineSimilarity := func(vecA, vecB map[string]int) float64 {
		dotProduct := 0
		normA := 0
		normB := 0

		for word, countA := range vecA {
			if countB, found := vecB[word]; found {
				dotProduct += countA * countB
			}
			normA += countA * countA
		}

		for _, countB := range vecB {
			normB += countB * countB
		}

		if normA == 0 || normB == 0 {
			return 0.0 // Zero similarity if one of the vectors is zero
		}

		return float64(dotProduct) / (math.Sqrt(float64(normA)) * math.Sqrt(float64(normB)))
	}

	// Iterate over all brands
	for rowsBrands.Next() {
		var brandName string
		if err := rowsBrands.Scan(&brandName); err != nil {
			logger.LogError("Failed to scan brand name")
			return
		}

		wg.Add(1) // Add to the WaitGroup for each brand

		// Launch a goroutine for each brand
		go func(brandName string) {
			defer wg.Done() // Mark the goroutine as done when finished

			// Step 1: Delete data where articles are published more than 15 days ago for the current brand
			deleteOldDataQuery := `
				DELETE FROM content_based_articles
				WHERE article_url_1 IN (
					SELECT cba.article_url_1
					FROM content_based_articles AS cba
					JOIN page AS p ON p.url = cba.article_url_1
					WHERE cba.brand = $1
					AND p.brand = $1
					AND p.publication_date < NOW() - INTERVAL '15 DAYS'
				);
			`
			_, err := db.Exec(deleteOldDataQuery, brandName)
			if err != nil {
				logger.LogError("Failed to delete old data: %v", err)
				return
			} else {
				logger.LogInfo("Successfully deleted old content-based articles for brand: %s", brandName)
			}

			// Step 1: Delete data where articles are published more than 15 days ago for the current brand
			deleteOldDataQuery = `
				DELETE FROM content_based_articles
				WHERE article_url_2 IN (
					SELECT cba.article_url_2
					FROM content_based_articles AS cba
					JOIN page AS p ON p.url = cba.article_url_2
					WHERE cba.brand = $1
					AND p.brand = $1
					AND p.publication_date < NOW() - INTERVAL '15 DAYS'
				);
			`
			_, err = db.Exec(deleteOldDataQuery, brandName)
			if err != nil {
				logger.LogError("Failed to delete old data: %v", err)
				return
			} else {
				logger.LogInfo("Successfully deleted old content-based articles for brand: %s", brandName)
			}

			// Retrieve all articles for the current brand
			rowsArticles, err := db.Query("SELECT url, content_vector FROM page WHERE brand = $1 AND type = 'article' AND publication_date >= NOW() - INTERVAL '15 DAYS'", brandName)
			if err != nil {
				logger.LogError("Failed to query articles for brand %s", brandName)
				return
			}
			defer rowsArticles.Close()

			var articles []struct {
				Url           string
				ContentVector map[string]int
			}

			// Collect all articles and their vectors for the current brand
			for rowsArticles.Next() {
				var articleUrl string
				var contentVectorJSON []byte
				if err := rowsArticles.Scan(&articleUrl, &contentVectorJSON); err != nil {
					logger.LogError("Failed to scan article row for brand %s", brandName)
					return
				}

				// Deserialize JSON content vector
				var contentVector map[string]int
				if err := json.Unmarshal(contentVectorJSON, &contentVector); err != nil {
					logger.LogError("Failed to unmarshal article content vector for brand %s", brandName)
					return
				}

				articles = append(articles, struct {
					Url           string
					ContentVector map[string]int
				}{
					Url:           articleUrl,
					ContentVector: contentVector,
				})
			}

			// Compare each article with all others in the same brand
			for i := 0; i < len(articles); i++ {
				for j := i + 1; j < len(articles); j++ {
					// Calculate similarity between article i and article j
					similarity := cosineSimilarity(articles[i].ContentVector, articles[j].ContentVector)
					roundedSimilarity := math.Round(similarity*100) / 100

					// Insert similarity into the content_based_articles table
					query := `
						INSERT INTO content_based_articles (brand, article_url_1, article_url_2, similarity_score)
						VALUES ($1, $2, $3, $4)
						ON CONFLICT (brand, article_url_1, article_url_2)
						DO NOTHING;
					`
					_, err := db.Exec(query,
						brandName, articles[i].Url, articles[j].Url, roundedSimilarity)
					if err != nil {
						logger.LogError("Failed to insert similarity score for %s and %s: %v\n", articles[i].Url, articles[j].Url, err)
						continue
					}

					// Also insert the reverse relationship (article j -> article i)
					_, err = db.Exec(query,
						brandName, articles[j].Url, articles[i].Url, roundedSimilarity)
					if err != nil {
						logger.LogError("Failed to insert similarity score for %s and %s: %v\n", articles[j].Url, articles[i].Url, err)
					}
				}
			}

			logger.LogInfo("Content-based articles generated for brand: %s", brandName)
		}(brandName) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Content-based articles generated for all brands.")
}
