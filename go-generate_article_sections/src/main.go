package main

import (
	"database/sql"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var (
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
	// Query to get distinct brands from the brand table
	brandsQuery := "SELECT name FROM brand"
	brandsRows, err := db.Query(brandsQuery)
	if err != nil {
		logger.LogError("Failed to retrieve brands: %v", err)
		return
	}
	defer brandsRows.Close()

	var wg sync.WaitGroup

	// Iterate over each brand
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

			// Query to get distinct section/sub_section pairs for the brand
			sectionsQuery := `
            SELECT DISTINCT section, sub_section
            FROM page
            WHERE 
				brand = $1
				AND publication_date >= CURRENT_DATE - INTERVAL '1 MINUTES'
				AND publication_date < CURRENT_DATE
        `
			sectionsRows, err := db.Query(sectionsQuery, brand)
			if err != nil {
				logger.LogError("Failed to retrieve sections for brand %s: %v", brand, err)
				return
			}
			defer sectionsRows.Close()

			// Iterate over each section/sub_section pair
			for sectionsRows.Next() {
				var section string
				var subSection *string
				if err := sectionsRows.Scan(&section, &subSection); err != nil {
					logger.LogError("Failed to scan section/sub_section for brand %s: %v", brand, err)
					return
				}

				// Insert the section/sub_section pair if it does not exist
				insertQuery := `
				INSERT INTO article_section (brand, section, sub_section)
				VALUES ($1, $2, $3)
				ON CONFLICT (brand, section, sub_section)
				DO NOTHING;
			`
				_, err := db.Exec(insertQuery, brand, section, subSection)
				if err != nil {
					logger.LogError("Failed to insert section/sub_section for brand %s: %v", section, brand, err)
					return
				}
			}

			logger.LogInfo("Sections and sub-sections updated successfully for brand %s", brand)
		}(brand) // Pass the brand as an argument to the goroutine
	}

	// Wait for all goroutines to complete
	wg.Wait()

	logger.LogInfo("Sections and sub-sections updated successfully.")
}
