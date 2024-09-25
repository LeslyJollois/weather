package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/reiver/go-porterstemmer"
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
	type Page struct {
		Url     string
		Content string
	}

	// Helper function to stem and tokenize the content
	stemAndTokenize := func(content string) []string {
		words := strings.Fields(strings.ToLower(content))
		var stemmedWords []string

		for _, word := range words {
			stemmedWord := porterstemmer.StemString(word)
			stemmedWords = append(stemmedWords, stemmedWord)
		}
		return stemmedWords
	}

	// Helper function to generate a frequency vector from stemmed words
	generateContentVector := func(words []string) map[string]int {
		wordFreq := make(map[string]int)

		for _, word := range words {
			wordFreq[word]++
		}

		return wordFreq
	}

	// Helper function to update page vector in the database
	updatePageVector := func(pageUrl string, vector map[string]int) error {
		vectorJSON, err := json.Marshal(vector)
		if err != nil {
			return fmt.Errorf("failed to marshal vector: %v", err)
		}

		query := `UPDATE page SET content_vector = $1 WHERE url = $2`
		_, err = db.Exec(query, vectorJSON, pageUrl)
		if err != nil {
			return fmt.Errorf("failed to update page vector: %v", err)
		}

		return nil
	}

	// Retrieve all distinct brands from the page table
	brandRows, err := db.Query("SELECT name FROM brand")
	if err != nil {
		logger.LogError("Failed to query database for brands")
		return
	}
	defer brandRows.Close()

	var wg sync.WaitGroup

	for brandRows.Next() {
		var brandName string
		if err := brandRows.Scan(&brandName); err != nil {
			logger.LogError("Failed to scan brand")
			return
		}

		wg.Add(1) // Add to the WaitGroup for each brand

		// Launch a goroutine for each brand
		go func(brandName string) {
			defer wg.Done() // Mark the goroutine as done when finished

			// Retrieve all pages for the current brand where the vector is NULL
			pageRows, err := db.Query("SELECT url, content FROM page WHERE brand = $1 AND type = 'article' AND content_vector IS NULL", brandName)
			if err != nil {
				logger.LogError("Failed to query pages for brand %s", brandName)
				return
			}
			defer pageRows.Close()

			for pageRows.Next() {
				var page Page
				if err := pageRows.Scan(&page.Url, &page.Content); err != nil {
					logger.LogError("Failed to scan page row for brand %s", brandName)
					return
				}

				// Tokenize, stem the content, and generate frequency vector
				stemmedWords := stemAndTokenize(page.Content)
				contentVector := generateContentVector(stemmedWords)

				// Update the vector in the database
				if err := updatePageVector(page.Url, contentVector); err != nil {
					logger.LogError("Failed to update page vector for brand %s: %v", brandName, err)
					return
				}
			}

			pageRows.Close()

			logger.LogInfo("Content vectors generated and stored successfully for brand: %s", brandName)
		}(brandName) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Content vectors generated and stored successfully for all brands")
}
