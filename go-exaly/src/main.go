package main

import (
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var (
	logger *Logger
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

// ServeHTML serves the HTML page for the site home
func ServeSiteHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../assets/html/site/index.html")
}

// ServeHTML serves the HTML page for the site home
func ServeSiteWeather(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../../html/site/weather.html")
}

// ServeHTML serves the HTML page for the site home
func ServeSiteLogo(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../../images/site/logo.png")
}

// ServeHTML serves the HTML page for the site home
func ServeSiteLogoTextOnly(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../../images/site/logo-text-only.png")
}

// ServeHTML serves the HTML page for the site home
func ServeSiteBanner(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../../images/site/banner.jpg")
}

// ServeHTML serves the HTML page for the site home
func ServeSiteLogoWeather(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../../images/site/logo-weather.png")
}

// ServeHTML serves the HTML page for the test home
func ServeTestHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../assets/html/test/home.html")
}

// ServeHTML serves the HTML page for the test article 1
func ServeTestArticle1(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../assets/html/test/article1.html")
}

// ServeHTML serves the HTML page for the test article 2
func ServeTestArticle2(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "../assets/html/test/article2.html")
}

// Initialize Redis and SQL clients
func init() {
	// Init logger
	logger = &Logger{
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}

	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		logger.LogFatal("[SYSTEM] Error loading .env file")
	}
}

// Main function to start the server
func main() {
	// Site
	http.HandleFunc("/", ServeSiteHome)
	http.HandleFunc("/weather", ServeSiteWeather)
	http.HandleFunc("/images/logo.png", ServeSiteLogo)
	http.HandleFunc("/images/logo-text-only.png", ServeSiteLogoTextOnly)
	http.HandleFunc("/images/banner.jpg", ServeSiteBanner)
	http.HandleFunc("/images/logo-weather.png", ServeSiteLogoWeather)

	// Exemple pages
	http.HandleFunc("/test/home", ServeTestHome)
	http.HandleFunc("/test/article-1", ServeTestArticle1)
	http.HandleFunc("/test/article-2", ServeTestArticle2)

	// Use the PORT environment variable or default to 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	logger.LogInfo("[SYSTEM] Server started on port :%s", port)
	logger.LogFatal("[SYSTEM] " + http.ListenAndServe(":"+port, nil).Error())
}
