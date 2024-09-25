package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	referrerparser "github.com/snowplow-referer-parser/golang-referer-parser"
	"github.com/tdewolff/minify/v2"
	"github.com/tdewolff/minify/v2/js"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var (
	ctx         = context.Background()
	logger      *Logger
	redisClient *redis.Client
	db          *sql.DB
	psClient    *pubsub.Client

	// Allowed lead event names
	allowedLeadEvents = map[string]bool{
		"page_view":     true,
		"page_behavior": true,
	}
)

type Brand struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	SiteHost string `json:"site_host"`
}

// Structs for storing page data
type PageData struct {
	URL              string               `json:"url"`
	Type             string               `json:"type"`
	Language         string               `json:"language"`
	PublicationDate  PublicationDateTime  `json:"publicationDate"`
	ModificationDate *PublicationDateTime `json:"modificationDate"`
	Title            string               `json:"title"`
	Description      string               `json:"description"`
	Content          string               `json:"content"`
	Section          string               `json:"section"`
	SubSection       *string              `json:"subSection"`
	Image            *string              `json:"image"`
	IsPaid           bool                 `json:"isPaid"`
}

// Structs for storing page data
type PageDataPubSub struct {
	DateTime         time.Time  `json:"datetime"`
	Brand            string     `json:"brand"`
	URL              string     `json:"url"`
	Type             string     `json:"type"`
	Language         string     `json:"language"`
	PublicationDate  time.Time  `json:"publication_date"`
	ModificationDate *time.Time `json:"modification_date"`
	Title            string     `json:"title"`
	Description      string     `json:"description"`
	Content          string     `json:"content"`
	Section          string     `json:"section"`
	SubSection       *string    `json:"sub_section"`
	Image            *string    `json:"image"`
	IsPaid           bool       `json:"is_paid"`
}

// Structs for storing user data
type UserData struct {
	LeadUUID     string `json:"leadUuid"`
	UserID       string `json:"userID"`
	Email        string `json:"email"`
	FirstName    string `json:"firstName"`
	LastName     string `json:"lastName"`
	IsSubscriber bool   `json:"isSubscriber"`
}

// Structs for storing user data
type UserDataPubSub struct {
	DateTime     time.Time `json:"datetime"`
	Brand        string    `json:"brand"`
	LeadUUID     string    `json:"lead_uuid"`
	UserID       string    `json:"user_id"`
	Email        string    `json:"email"`
	FirstName    string    `json:"first_name"`
	LastName     string    `json:"last_name"`
	IsSubscriber bool      `json:"is_subscriber"`
}

// Structs for storing lead event data
type LeadEventData struct {
	UUID             string                 `json:"uuid"`
	LeadUUID         string                 `json:"leadUuid"`
	Name             string                 `json:"name"`
	PageType         string                 `json:"pageType"`
	PageLanguage     string                 `json:"pageLanguage"`
	Device           string                 `json:"device"`
	Url              string                 `json:"url"`
	Referrer         string                 `json:"referrer"`
	ReferrerType     string                 `json:"referrer_type"`
	RelevantReferrer string                 `json:"relevantReferrer"`
	Metas            map[string]interface{} `json:"metas"`
	Consent          bool                   `json:"consent"`
}

// Structs for storing lead event data
type LeadEventDataPubSub struct {
	Brand            string                 `json:"brand"`
	UUID             string                 `json:"uuid"`
	LeadUUID         string                 `json:"lead_uuid"`
	Name             string                 `json:"name"`
	PageType         string                 `json:"page_type"`
	PageLanguage     string                 `json:"page_language"`
	Device           string                 `json:"device"`
	Url              string                 `json:"url"`
	Referrer         string                 `json:"referrer"`
	ReferrerType     string                 `json:"referrer_type"`
	RelevantReferrer string                 `json:"relevant_referrer"`
	Metas            map[string]interface{} `json:"metas"`
	Consent          bool                   `json:"consent"`
	IP               string                 `json:"ip"`
}

// Struct for page metrics
type PageMetrics struct {
	URL            string  `json:"url"`
	ViewCount      int     `json:"view_count"`
	AvgTimeSpent   float64 `json:"avg_time_spent"`
	AvgReadingRate float64 `json:"avg_reading_rate"`
}

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

type PublicationDateTime time.Time

const publicationDataTimeFormat = "2006-01-02T15:04:05Z07:00"

func (ct *PublicationDateTime) UnmarshalJSON(data []byte) error {
	str := string(data)
	if str == "null" {
		*ct = PublicationDateTime(time.Time{})
		return nil
	}

	t, err := time.Parse(`"`+publicationDataTimeFormat+`"`, str)
	if err != nil {
		return err
	}
	*ct = PublicationDateTime(t)
	return nil
}

func (ct PublicationDateTime) Time() time.Time {
	return time.Time(ct)
}

func isUserAgentBlocked(userAgent string) (bool, error) {
	// List of blocked user agents
	blockedUserAgents := []string{
		"curl",
		"wget",
		"PostmanRuntime",
		"Apache-HttpClient",
		"Python-urllib",
		"Java/1.",
		"libwww-perl",
		"PHP",
		"Go-http-client",
		"HTTrack",
		"Rogue Wave",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; SV1; .NET CLR 1.1.4322)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.0.3705)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 3.0.04506.30)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 3.5.30729)",
		"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Trident/4.0)",
		"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
		"Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
		"Mozilla/4.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)",
		"Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)",
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)",
		"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
		"Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
	}

	// Check blocked user agents
	for _, blocked := range blockedUserAgents {
		if strings.Contains(userAgent, blocked) {
			return true, fmt.Errorf("User agent is blocked: %s", userAgent)
		}
	}

	return false, nil
}

// getBrandFromHost retrieves the brand details for a given host using Redis cache.
func getBrandFromHost(host string) (*Brand, error) {
	var brand Brand

	// Check Redis cache
	cacheKey := fmt.Sprintf("brand:%s", host)
	cachedBrand, err := redisClient.Get(ctx, cacheKey).Result()
	if err != redis.Nil && err == nil {
		if err := json.Unmarshal([]byte(cachedBrand), &brand); err != nil {
			logger.LogError("[BRAND] Error unmarshalling brand: %v", err)
			return nil, fmt.Errorf("Error unmarshalling brand: %v", err)
		}

		return &brand, nil
	}

	brand.Host = host

	// Values not found in cache, retrieve from database
	err = db.QueryRow("SELECT name, site_host FROM brand WHERE host = $1", host).Scan(&brand.Name, &brand.SiteHost)
	if err != nil {
		logger.LogError("[BRAND] Error querying database: %v", err)
		return nil, fmt.Errorf("Error querying database: %v", err)
	}

	// Convert the page data to JSON
	brandJSON, err := json.Marshal(brand)
	if err != nil {
		logger.LogError("[BRAND] Error marshalling brand: %v", err)
		return nil, fmt.Errorf("Error marshalling brand: %v", err)
	}

	// Cache the result with a 1-hour TTL
	err = redisClient.Set(ctx, cacheKey, brandJSON, 1*time.Hour).Err()
	if err != nil {
		logger.LogError("[BRAND] Error setting cache: %v", err)
	}

	return &brand, nil
}

// Health check
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func isCollectAllowed(r *http.Request) (*Brand, int, error) {
	if r.Method != http.MethodPost {
		return nil, http.StatusMethodNotAllowed, errors.New("Invalid request method")
	}

	// Check if the user agent is blocked
	userAgentBlocked, err := isUserAgentBlocked(r.UserAgent())
	if err != nil {
		return nil, http.StatusForbidden, err
	}

	if userAgentBlocked {
		return nil, http.StatusForbidden, err
	}

	// Extract host from the request's Host header
	host := r.Host
	if host == "" {
		return nil, http.StatusBadRequest, errors.New("Host header is required")
	}

	// Get the brand name
	brand, err := getBrandFromHost(host)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New(fmt.Sprintf("Error getting brand: %v", err))
	}

	// Check if the Origin header matches the Host header
	origin := r.Header.Get("Origin")
	if origin != "" {
		parsedOrigin, err := url.Parse(origin)
		if err != nil {
			return nil, http.StatusBadRequest, errors.New("Invalid Origin header")
		}

		if parsedOrigin.Host != brand.SiteHost && parsedOrigin.Host != brand.Host {
			return nil, http.StatusForbidden, errors.New("Origin header must match Site host")
		}
	}

	return brand, 0, nil
}

// Collect Page Data
func collectPageDataHandler(w http.ResponseWriter, r *http.Request) {
	brand, errorCode, err := isCollectAllowed(r)
	if err != nil {
		logger.LogError("[COLLECT][PAGE] Collect is not allowed: %v", err)
		http.Error(w, err.Error(), errorCode)
		return
	}

	var pageData PageData
	if err := json.NewDecoder(r.Body).Decode(&pageData); err != nil {
		logger.LogError("[COLLECT][PAGE] Invalid request payload, error: %v", err)
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Log initial indiquant le début de la collecte des données
	logger.LogInfo("[COLLECT][PAGE] Collecting page data for URL: %s", pageData.URL)

	modificationDateString := ""

	if pageData.ModificationDate != nil {
		modificationDateString = pageData.ModificationDate.Time().String()
	}

	// Check cache
	cacheKey := fmt.Sprintf("page:%s:%s:%s", brand.Name, pageData.URL, modificationDateString)
	_, err = redisClient.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		// Cache miss, publish the page data
		logger.LogInfo("[COLLECT][PAGE] Cache miss for page: %s", pageData.URL)

		logger.LogInfo("[COLLECT][PAGE] Publishing new page data for URL: %s", pageData.URL)
		err := publishPageData(brand.Name, pageData)
		if err != nil {
			logger.LogError("[COLLECT][PAGE] Failed to publish page data: %v", err)
		}

		// Set cache with TTL of 10 minutes
		err = redisClient.Set(ctx, cacheKey, "exists", 10*time.Minute).Err()
		if err != nil {
			logger.LogError("[COLLECT][PAGE] Failed to set cache for page: %s, error: %v", pageData.URL, err)
		} else {
			logger.LogInfo("[COLLECT][PAGE] Cache set for page: %s with TTL 24 hours", pageData.URL)
		}
	} else if err != nil {
		logger.LogError("[COLLECT][PAGE] Failed to check cache for page: %s, error: %v", pageData.URL, err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	} else {
		logger.LogInfo("[COLLECT][PAGE] Page already exists in Redis cache for URL: %s", pageData.URL)
		// Data already processed
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Publish Page Data to Pub/Sub
func publishPageData(brandName string, pageData PageData) error {
	var modificationDate *time.Time

	if pageData.ModificationDate != nil {
		time := pageData.ModificationDate.Time()
		modificationDate = &time
	}

	pageDataPubSub := PageDataPubSub{
		DateTime:         time.Now().UTC(),
		Brand:            brandName,
		URL:              pageData.URL,
		Type:             pageData.Type,
		Language:         pageData.Language,
		PublicationDate:  pageData.PublicationDate.Time(),
		ModificationDate: modificationDate,
		Title:            pageData.Title,
		Description:      pageData.Description,
		Content:          pageData.Content,
		Section:          pageData.Section,
		SubSection:       pageData.SubSection,
		Image:            pageData.Image,
		IsPaid:           pageData.IsPaid,
	}

	// Convert the page data to JSON
	pageDataPubSubJSON, err := json.Marshal(pageDataPubSub)
	if err != nil {
		return err
	}

	// Publish the message to the Pub/Sub topic asynchronously
	topic := psClient.Topic(os.Getenv("ENV") + "-page")
	result := topic.Publish(context.Background(), &pubsub.Message{
		Data: pageDataPubSubJSON,
	})

	// Log any errors from the publishing result
	_, err = result.Get(context.Background())
	if err != nil {
		return err
	}

	return nil
}

// Collect User Data
func collectUserDataHandler(w http.ResponseWriter, r *http.Request) {
	brand, errorCode, err := isCollectAllowed(r)
	if err != nil {
		logger.LogError("[COLLECT][USER] Collect is not allowed: %v", err)
		http.Error(w, err.Error(), errorCode)
		return
	}

	var userData UserData
	if err := json.NewDecoder(r.Body).Decode(&userData); err != nil {
		logger.LogError("[COLLECT][USER] Invalid request payload: %v", err)
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Check cache
	cacheKey := fmt.Sprintf("user_data:%s:%s:%s", brand.Name, userData.LeadUUID, userData.IsSubscriber)
	_, err = redisClient.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		// Cache miss, check database
		logger.LogInfo("[COLLECT][USER] Cache miss for Lead UUID: %s", userData.LeadUUID)

		// User not found in database, insert the data
		logger.LogInfo("[COLLECT][USER] Publishing new user data for Lead UUID: %s", userData.LeadUUID)
		err := publishUserData(brand.Name, userData)
		if err != nil {
			logger.LogError("[COLLECT][USER] Failed to insert user data: %v", err)
		}

		// Set cache with TTL of 1 second
		err = redisClient.Set(ctx, cacheKey, "exists", 1*time.Second).Err()
		if err != nil {
			logger.LogError("[COLLECT][USER] Failed to set cache for Lead UUID: %s, error: %v", userData.LeadUUID, err)
		}

		// Set cache in case of cache miss
		err = redisClient.Set(ctx, cacheKey, "exists", 1*time.Second).Err()
		if err != nil {
			logger.LogError("[COLLECT][USER] Failed to set cache for Lead UUID: %s, error: %v", userData.LeadUUID, err)
		}
	} else if err != nil {
		logger.LogError("[COLLECT][USER] Internal server error while checking cache: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	} else {
		logger.LogInfo("[COLLECT][USER] User already exists in Redis cache: %s", userData.LeadUUID)
		// Data already processed
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Publish Page Data to Pub/Sub
func publishUserData(brandName string, userData UserData) error {
	userDataPubSub := UserDataPubSub{
		DateTime:     time.Now().UTC(),
		Brand:        brandName,
		LeadUUID:     userData.LeadUUID,
		UserID:       userData.UserID,
		Email:        userData.Email,
		FirstName:    userData.FirstName,
		LastName:     userData.LastName,
		IsSubscriber: userData.IsSubscriber,
	}

	// Convert the user data to JSON
	pageDataPubSubJSON, err := json.Marshal(userDataPubSub)
	if err != nil {
		return err
	}

	// Publish the message to the Pub/Sub topic asynchronously
	topic := psClient.Topic(os.Getenv("ENV") + "-user")
	result := topic.Publish(context.Background(), &pubsub.Message{
		Data: pageDataPubSubJSON,
	})

	// Log any errors from the publishing result
	_, err = result.Get(context.Background())
	if err != nil {
		return err
	}

	return nil
}

// Collect Lead Event Data
func collectLeadEventDataHandler(w http.ResponseWriter, r *http.Request) {
	brand, errorCode, err := isCollectAllowed(r)
	if err != nil {
		logger.LogError("[COLLECT][LEAD_EVENT] Error in isCollectAllowed: %v", err)
		http.Error(w, err.Error(), errorCode)
		return
	}

	var leadEventData LeadEventData
	if err := json.NewDecoder(r.Body).Decode(&leadEventData); err != nil {
		logger.LogError("[COLLECT][LEAD_EVENT] Invalid request payload: %v", err)
		http.Error(w, "Invalid request payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate event name
	if !allowedLeadEvents[leadEventData.Name] {
		logger.LogError("[COLLECT][LEAD_EVENT] Invalid lead event name: %s", leadEventData.Name)
		http.Error(w, "Invalid lead event name", http.StatusBadRequest)
		return
	}

	// Check if Lead UUID is present
	if leadEventData.LeadUUID == "" {
		// Generate new Lead UUID if not present
		leadEventData.LeadUUID = generateUUID()
		http.SetCookie(w, &http.Cookie{
			Name:    "lead-uuid",
			Value:   leadEventData.LeadUUID,
			Path:    "/",
			Expires: time.Now().Add(365 * 24 * time.Hour), // Cookie valid for 1 year
		})
		logger.LogInfo("[COLLECT][LEAD_EVENT] Generated new Lead UUID: %s", leadEventData.LeadUUID)
	}

	// Check cache
	cacheKey := fmt.Sprintf("lead_event:%s:%s:%s:%s", brand.Name, leadEventData.LeadUUID, leadEventData.Name, leadEventData.Url)
	_, err = redisClient.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		// Set cache with TTL of 10 seconds
		err = redisClient.Set(ctx, cacheKey, "exists", 10*time.Second).Err()
		if err != nil {
			logger.LogError("[COLLECT][PAGE] Failed to set cache of lead event for brand %s, leadUuid: %s, name: %s, url: %s, error: %v", brand.Name, leadEventData.LeadUUID, leadEventData.Name, leadEventData.Url, err)
		}
	} else if err != nil {
		logger.LogError("[COLLECT][PAGE] Failed to get cache of lead event for brand %s, leadUuid: %s, name: %s, url: %s", brand.Name, leadEventData.LeadUUID, leadEventData.Name, leadEventData.Url)
		w.WriteHeader(http.StatusInternalServerError)
		return
	} else {
		logger.LogInfo("[COLLECT][PAGE] Lead event already collected for brand %s, leadUuid: %s, name: %s, url: %s", brand.Name, leadEventData.LeadUUID, leadEventData.Name, leadEventData.Url)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if leadEventData.Name == "page_view" {
		if leadEventData.Referrer == "" {
			leadEventData.ReferrerType = "direct"
		} else {
			var parseError bool

			parsedUrl, err := url.Parse(leadEventData.Url)
			if err != nil {
				logger.LogError("[COLLECT][LEAD_EVENT] Unable to parse page url")
				parseError = true
			}

			parsedReferrer, err := url.Parse(leadEventData.Referrer)
			if err != nil {
				logger.LogError("[COLLECT][LEAD_EVENT] Unable to parse referrer url")
				parseError = true
			}

			if !parseError && parsedUrl.Host == parsedReferrer.Host {
				leadEventData.ReferrerType = "internal"
			} else {
				parsedReferrer := referrerparser.Parse(leadEventData.Referrer)
				leadEventData.ReferrerType = parsedReferrer.Medium
			}
		}
	}

	logger.LogInfo("[COLLECT][LEAD_EVENT] Publishing lead event data for Lead UUID: %s and Event UUID: %s", leadEventData.LeadUUID, leadEventData.UUID)

	clientIp := ""
	if leadEventData.Consent {
		clientIp = getClientIP(r)
	}

	err = publishLeadEventData(brand.Name, leadEventData, clientIp)
	if err != nil {
		logger.LogError("[COLLECT][LEAD_EVENT] Failed to publish lead event data: %v", err)
	}

	w.WriteHeader(http.StatusNoContent)
}

// publishLeadEventData sends lead event data to a Pub/Sub topic asynchronously
func publishLeadEventData(brandName string, leadEventData LeadEventData, clientIp string) error {
	leadEventDataPubSub := LeadEventDataPubSub{
		Brand:            brandName,
		UUID:             leadEventData.UUID,
		LeadUUID:         leadEventData.LeadUUID,
		Name:             leadEventData.Name,
		PageType:         leadEventData.PageType,
		PageLanguage:     leadEventData.PageLanguage,
		Device:           leadEventData.Device,
		Url:              leadEventData.Url,
		Referrer:         leadEventData.Referrer,
		ReferrerType:     leadEventData.ReferrerType,
		RelevantReferrer: leadEventData.RelevantReferrer,
		Metas:            leadEventData.Metas,
		Consent:          leadEventData.Consent,
		IP:               clientIp,
	}

	// Convert the lead event data to JSON
	leadEventDataPubSubJSON, err := json.Marshal(leadEventDataPubSub)
	if err != nil {
		return err
	}

	// Publish the message to the Pub/Sub topic asynchronously
	topic := psClient.Topic(os.Getenv("ENV") + "-lead_event")
	result := topic.Publish(context.Background(), &pubsub.Message{
		Data: leadEventDataPubSubJSON,
	})

	// Log any errors from the publishing result
	_, err = result.Get(context.Background())
	if err != nil {
		return err
	}

	return nil
}

// Handler to recommend similar articles based on precomputed similarities
func getArticleContentBasedArticlesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Extract host from the request's Host header
	host := r.Host
	if host == "" {
		http.Error(w, "Host header is required", http.StatusBadRequest)
		return
	}

	// Get the brand name
	brand, err := getBrandFromHost(host)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting brand: %v", err), http.StatusInternalServerError)
		return
	}

	url := r.URL.Query().Get("url")
	if url == "" {
		http.Error(w, "Missing article url", http.StatusBadRequest)
		return
	}

	// Check cache first
	cacheKey := fmt.Sprintf("similar_articles:%s:%s", brand.Name, url)
	cachedResponse, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(cachedResponse))
		return
	}

	// Retrieve similar articles from the content_based_articles table
	rows, err := db.Query(`
		SELECT 
			p.url, 
			p.title, 
			p.description, 
			p.section, 
			p.sub_section, 
			p.image, 
			cba.similarity_score 
		FROM 
			content_based_articles cba
		JOIN 
			page p ON p.url = cba.article_url_2 AND p.brand = $1
		WHERE 
			cba.brand = $1
			AND cba.article_url_1 = $2
			AND cba.similarity_score > 0
		ORDER BY 
			cba.similarity_score DESC
		LIMIT 10`, brand.Name, url)
	if err != nil {
		http.Error(w, "Failed to query similar articles", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Define the structure to hold similar articles
	type SimilarArticle struct {
		Url         string  `json:"url"`
		Title       string  `json:"title"`
		Description string  `json:"description"`
		Section     string  `json:"section"`
		SubSection  *string `json:"sub_section"`
		Image       *string `json:"image"`
		Similarity  float64 `json:"similarity"`
	}

	var similarArticles []SimilarArticle

	// Iterate through the results and collect similar articles
	for rows.Next() {
		var article SimilarArticle
		if err := rows.Scan(&article.Url, &article.Title, &article.Description, &article.Section, &article.SubSection, &article.Image, &article.Similarity); err != nil {
			http.Error(w, "Failed to scan article row", http.StatusInternalServerError)
			return
		}
		similarArticles = append(similarArticles, article)
	}

	// Respond with similar articles
	response, err := json.Marshal(similarArticles)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	// Cache the response with a TTL of 1 second
	if err := redisClient.Set(ctx, cacheKey, response, 1*time.Second).Err(); err != nil {
		log.Println("Failed to set cache:", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

// Route handler to get metrics for a specific article with optional date filtering and "dump" parameter
func getArticleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Extract host from the request's Host header
	host := r.Host
	if host == "" {
		http.Error(w, "Host header is required", http.StatusBadRequest)
		return
	}

	// Get the brand name
	brand, err := getBrandFromHost(host)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting brand: %v", err), http.StatusInternalServerError)
		return
	}

	pageURL := r.URL.Query().Get("url")
	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")
	dump := r.URL.Query().Get("dump") == "1"
	dumpRange := r.URL.Query().Get("dump_range")

	if pageURL == "" {
		http.Error(w, "Missing page URL", http.StatusBadRequest)
		return
	}

	// Check if the metrics for this URL are in the Redis cache, including optional dates in cache key
	cacheKey := fmt.Sprintf("article_metrics:%s:%s:%s:%s:%t:%s", brand.Name, pageURL, startDate, endDate, dump, dumpRange)
	cachedData, err := redisClient.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		var metrics interface{} // Can be either aggregated or periodical metrics

		var args []interface{}
		args = append(args, brand.Name, pageURL)

		if dump {
			// Determine the period for aggregation
			var timeTrunc string
			switch dumpRange {
			case "day":
				timeTrunc = "day"
			case "month":
				timeTrunc = "month"
			default:
				// Default to hour if no valid period is provided
				timeTrunc = "hour"
			}

			// Query to return metrics aggregated by the chosen period
			query := fmt.Sprintf(`
				SELECT 
					DATE_TRUNC('%s', calculation_period) AS period,
					SUM(view_count) AS view_count,
					ROUND(AVG(avg_time_spent), 2) AS avg_time_spent,
					ROUND(AVG(avg_reading_rate), 2) AS avg_reading_rate,
					ROUND(
						(SUM(view_count) * 0.4) + 
						(AVG(avg_reading_rate) * 0.3) + 
						(AVG(avg_time_spent) * 0.3)
					) AS engagement_score
				FROM 
					article_metrics
				WHERE 
					brand = $1
					AND url = $2
					AND calculation_period >= NOW() - INTERVAL '90 DAYS'
					AND calculation_period < NOW()
			`, timeTrunc)

			if startDate != "" && endDate != "" {
				query += " AND calculation_period BETWEEN $3 AND $4"
			}
			query += " GROUP BY period ORDER BY period"

			rows, err := db.Query(query, args...)
			if err != nil {
				log.Printf("Error querying metrics for article: %v", err)
				http.Error(w, "Error querying article metrics", http.StatusInternalServerError)
				return
			}
			defer rows.Close()

			// Map to store the metrics aggregated by the chosen period with the period as the key
			periodicMetrics := make(map[string]map[string]interface{})

			for rows.Next() {
				var period time.Time
				var viewCount, engagementScore int
				var avgTimeSpent, avgReadingRate float64
				err = rows.Scan(&period, &viewCount, &avgTimeSpent, &avgReadingRate, &engagementScore)
				if err != nil {
					log.Printf("Error scanning periodic metrics: %v", err)
					http.Error(w, "Error reading article metrics", http.StatusInternalServerError)
					return
				}

				// Format the period as a string (hour/day/month)
				periodStr := period.Format("2006-01-02 15:00:00") // Default format is hour
				if dumpRange == "day" {
					periodStr = period.Format("2006-01-02") // Format as day
				} else if dumpRange == "month" {
					periodStr = period.Format("2006-01") // Format as month
				}

				// Assign the metrics to the corresponding period
				periodicMetrics[periodStr] = map[string]interface{}{
					"view_count":       viewCount,
					"avg_time_spent":   avgTimeSpent,
					"avg_reading_rate": avgReadingRate,
					"engagement_score": engagementScore,
				}
			}

			metrics = periodicMetrics
		} else {
			// Base query for aggregated metrics
			query := `
				SELECT 
					SUM(view_count) AS view_count,
					ROUND(AVG(avg_time_spent)) AS avg_time_spent,
					ROUND(AVG(avg_reading_rate)) AS avg_reading_rate,
					ROUND(
						(SUM(view_count) * 0.4) + 
						(AVG(avg_reading_rate) * 0.3) + 
						(AVG(avg_time_spent) * 0.3)
					) AS engagement_score
				FROM 
					article_metrics
				WHERE 
					brand = $1
					AND url = $2
			`

			// Modify the query if date filtering is applied
			if startDate != "" && endDate != "" {
				query += " AND calculation_period BETWEEN $3 AND $4"
				args = append(args, startDate, endDate)
			} else {
				query += " AND calculation_period <= NOW()"
			}

			var viewCount, avgTimeSpent, avgReadingRate, engagementScore int
			err := db.QueryRow(query, args...).Scan(&viewCount, &avgTimeSpent, &avgReadingRate, &engagementScore)
			if err != nil {
				log.Printf("Error querying metrics for article: %v", err)
				http.Error(w, "Error querying article metrics", http.StatusInternalServerError)
				return
			}

			metrics = map[string]interface{}{
				"view_count":       viewCount,
				"avg_time_spent":   avgTimeSpent,
				"avg_reading_rate": avgReadingRate,
				"engagement_score": engagementScore,
			}
		}

		// Convert the metrics to JSON for caching and response
		metricsJSON, err := json.Marshal(metrics)
		if err != nil {
			http.Error(w, "Error encoding JSON", http.StatusInternalServerError)
			return
		}

		// Store the result in Redis with an expiration time of 1s
		err = redisClient.Set(ctx, cacheKey, metricsJSON, 1*time.Second).Err()
		if err != nil {
			log.Printf("Error setting cache: %v", err)
		}

		// Respond with the JSON data
		w.Header().Set("Content-Type", "application/json")
		w.Write(metricsJSON)
	} else if err != nil {
		// Redis error
		log.Printf("Error accessing Redis: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	} else {
		// Cache hit, return the cached data
		log.Println("Cache hit for", cacheKey)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(cachedData))
	}
}

// getTopArticles returns the top 10 articles with the best engagement score, including article details
func getTopArticles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Extract host from the request's Host header
	host := r.Host
	if host == "" {
		http.Error(w, "Host header is required", http.StatusBadRequest)
		return
	}

	// Get the brand name
	brand, err := getBrandFromHost(host)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting brand: %v", err), http.StatusInternalServerError)
		return
	}

	// Retrieve GET parameters for section and sub_section
	section := r.URL.Query().Get("section")
	subSection := r.URL.Query().Get("sub_section")

	// Cache key for Redis, include section and sub_section if present
	cacheKey := fmt.Sprintf("top_articles:%s:%s:%s", brand.Name, section, subSection)

	// Try to retrieve articles from Redis cache
	cachedData, err := redisClient.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		// If the key does not exist in Redis, execute the SQL query

		// Build the WHERE clause for section and sub_section
		whereClause := "ta.brand = $1"
		params := []interface{}{brand.Name}

		if section != "" {
			whereClause += " AND ta.section = $2"
			params = append(params, section)
		} else {
			whereClause += " AND ta.section IS NULL"
		}
		if subSection != "" {
			whereClause += " AND ta.sub_section = $3"
			params = append(params, subSection)
		} else {
			whereClause += " AND ta.sub_section IS NULL"
		}

		// Construct the SQL query with the dynamic condition
		query := fmt.Sprintf(`
			SELECT 
				ta.url,
				p.title,
				p.description,
				p.image,
				p.section,
				p.sub_section,
				SUM(ta.view_count) AS view_count,
				ROUND(AVG(ta.avg_reading_rate), 2) AS avg_reading_rate,
				ROUND(AVG(ta.avg_time_spent), 2) AS avg_time_spent,
				ROUND(AVG(ta.recency_weight)) AS recency_weight,
				ROUND(
					AVG(ta.avg_reading_rate) * 0.3 +
					AVG(ta.avg_time_spent) * 0.3 +
					AVG(ta.recency_weight) * 0.4
				) AS engagement_score
			FROM 
				top_articles ta
			LEFT JOIN 
				page p ON p.url = ta.url AND p.brand = '%s'
			WHERE 
				%s
				AND ta.calculation_period >= NOW() - INTERVAL '2 DAY'
				AND ta.calculation_period < NOW()
			GROUP BY
				ta.url, p.title, p.description, p.image, p.section, p.sub_section
			ORDER BY 
				engagement_score DESC
			LIMIT 10
		`, brand.Name, whereClause)

		// Execute the SQL query with the parameters
		rows, err := db.Query(query, params...)
		if err != nil {
			log.Println(err.Error())
			http.Error(w, "Failed to query articles", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Prepare the response
		var articles []struct {
			URL             string  `json:"url"`
			Title           string  `json:"title"`
			Description     string  `json:"description"`
			Image           *string `json:"image"`
			Section         string  `json:"section"`
			SubSection      *string `json:"sub_section"`
			ViewCount       int     `json:"view_count"`
			AvgReadingRate  float64 `json:"avg_reading_rate"`
			AvgTimeSpent    float64 `json:"avg_time_spent"`
			RecencyWeight   float64 `json:"recency_weight"`
			EngagementScore float64 `json:"engagement_score"`
		}

		for rows.Next() {
			var article struct {
				URL             string  `json:"url"`
				Title           string  `json:"title"`
				Description     string  `json:"description"`
				Image           *string `json:"image"`
				Section         string  `json:"section"`
				SubSection      *string `json:"sub_section"`
				ViewCount       int     `json:"view_count"`
				AvgReadingRate  float64 `json:"avg_reading_rate"`
				AvgTimeSpent    float64 `json:"avg_time_spent"`
				RecencyWeight   float64 `json:"recency_weight"`
				EngagementScore float64 `json:"engagement_score"`
			}

			if err := rows.Scan(&article.URL, &article.Title, &article.Description, &article.Image, &article.Section, &article.SubSection, &article.ViewCount, &article.AvgReadingRate, &article.AvgTimeSpent, &article.RecencyWeight, &article.EngagementScore); err != nil {
				http.Error(w, "Failed to scan article", http.StatusInternalServerError)
				return
			}

			articles = append(articles, article)
		}

		// Convert the results to JSON
		articlesJSON, err := json.Marshal(articles)
		if err != nil {
			http.Error(w, "Failed to marshal articles", http.StatusInternalServerError)
			return
		}

		// Store the results in Redis with a TTL of 1 second
		err = redisClient.Set(ctx, cacheKey, articlesJSON, 1*time.Second).Err()
		if err != nil {
			http.Error(w, "Failed to cache articles", http.StatusInternalServerError)
			return
		}

		// Send the response
		w.Header().Set("Content-Type", "application/json")
		w.Write(articlesJSON)
	} else if err != nil {
		// If an error other than a missing key occurs
		http.Error(w, "Failed to retrieve cache", http.StatusInternalServerError)
		return
	} else {
		// If data is present in the Redis cache, send it directly
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(cachedData))
	}
}

func getArticleTopNextArticles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Extract host from the request's Host header
	host := r.Host
	if host == "" {
		http.Error(w, "Host header is required", http.StatusBadRequest)
		return
	}

	// Get the brand name
	brand, err := getBrandFromHost(host)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting brand: %v", err), http.StatusInternalServerError)
		return
	}

	// Parse query parameters
	url := r.URL.Query().Get("url")
	leadUuid := r.URL.Query().Get("lead_uuid")

	if url == "" {
		http.Error(w, "Missing 'url' parameter", http.StatusBadRequest)
		return
	}

	// Get the number of results from the query parameters
	numResults := r.URL.Query().Get("num_results")
	numResultsInt, err := strconv.Atoi(numResults)
	if err != nil || numResultsInt < 1 {
		numResultsInt = 10 // Default to 10 if the parameter is invalid or missing
	}
	if numResultsInt > 100 {
		numResultsInt = 100 // Limit to a maximum of 100 results
	}

	// Generate a cache key based on the URL, leadUuid (if provided), and number of results
	cacheKey := fmt.Sprintf("top_next_articles:%s:%s:%s:%d", brand.Name, url, leadUuid, numResultsInt)

	// Check Redis cache
	cachedData, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil && cachedData != "" {
		// Cache hit: return the cached data
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(cachedData))
		return
	}

	// Prepare the SQL query based on the presence of leadUuid
	var query string
	var args []interface{}
	var responseData []byte
	if leadUuid == "" {
		query = `
			SELECT 
				tna.next_url, 
				p.title AS title,
				p.description AS description,
				p.image AS image,
				p.section AS section,
				p.sub_section AS sub_section,
				SUM(tna.view_count) AS view_count,
				ROUND(AVG(tna.avg_reading_rate), 2) AS avg_reading_rate,
				ROUND(AVG(tna.avg_time_spent), 2) AS avg_time_spent,
				ROUND(
					(SUM(tna.view_count) * 0.4) + 
					(AVG(tna.avg_reading_rate) * 0.3) + 
					(AVG(tna.avg_time_spent) * 0.3)
				) AS engagement_score
			FROM 
				top_next_articles tna
			LEFT JOIN 
				page p ON tna.next_url = p.url AND p.brand = $1
			WHERE 
				tna.brand = $1
				AND tna.initial_url = $2
				AND tna.calculation_period >= NOW() - INTERVAL '2 DAY'
				AND tna.calculation_period < NOW()
			GROUP BY 
				tna.next_url, tna.view_count, tna.avg_reading_rate, tna.avg_time_spent, p.title, p.description, p.image, p.section, p.sub_section
			ORDER BY 
				engagement_score DESC
			LIMIT $3;
		`
		args = append(args, brand.Name, url, numResultsInt)

		// Execute the SQL query
		rows, err := db.Query(query, args...)
		if err != nil {
			log.Println(err.Error())
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Prepare the result set
		var articles []struct {
			URL             string  `json:"url"`
			Title           string  `json:"title"`
			Description     string  `json:"description"`
			Image           *string `json:"image"`
			Section         string  `json:"section"`
			SubSection      *string `json:"sub_section"`
			ViewCount       int     `json:"view_count"`
			AvgReadingRate  float64 `json:"avg_reading_rate"`
			AvgTimeSpent    float64 `json:"avg_time_spent"`
			EngagementScore int     `json:"engagement_score"`
		}

		// Process the result set
		for rows.Next() {
			var article struct {
				URL             string  `json:"url"`
				Title           string  `json:"title"`
				Description     string  `json:"description"`
				Image           *string `json:"image"`
				Section         string  `json:"section"`
				SubSection      *string `json:"sub_section"`
				ViewCount       int     `json:"view_count"`
				AvgReadingRate  float64 `json:"avg_reading_rate"`
				AvgTimeSpent    float64 `json:"avg_time_spent"`
				EngagementScore int     `json:"engagement_score"`
			}
			err := rows.Scan(&article.URL, &article.Title, &article.Description, &article.Image, &article.Section, &article.SubSection, &article.ViewCount, &article.AvgReadingRate, &article.AvgTimeSpent, &article.EngagementScore)
			if err != nil {
				log.Println(err.Error())
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}
			articles = append(articles, article)
		}

		// Convert the result to JSON
		responseData, err = json.Marshal(articles)
		if err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	} else {
		query = `
			SELECT 
				tna.next_url, 
				p.title AS title,
				p.description AS description,
				p.image AS image,
				p.section AS section,
				p.sub_section AS sub_section,
				SUM(tna.view_count) AS view_count,
				ROUND(AVG(tna.avg_reading_rate), 2) AS avg_reading_rate,
				ROUND(AVG(tna.avg_time_spent), 2) AS avg_time_spent,
				SUM(lsac.article_count) AS lead_articles_in_same_section,
				ROUND(
					(SUM(tna.view_count) * 0.4) + 
					(AVG(tna.avg_reading_rate) * 0.2) + 
					(AVG(tna.avg_time_spent) * 0.2) + 
					(SUM(lsac.article_count) * 0.2)
				) AS engagement_score
			FROM 
				top_next_articles tna
			LEFT JOIN 
				page p ON tna.next_url = p.url AND p.brand = $1
			LEFT JOIN 
				lead_read_articles AS lra ON lra.lead_uuid = $2 AND lra.brand = $1 AND lra.url = tna.next_url
			LEFT JOIN 
				lead_section_article_count AS lsac ON lsac.lead_uuid = $2 AND lsac.brand = $1 AND lsac.section = p.section
			WHERE 
				tna.brand = $1
				AND tna.initial_url = $3
				AND lra.url IS NULL
				AND tna.calculation_period >= NOW() - INTERVAL '2 DAY'
				AND tna.calculation_period < NOW()
			GROUP BY 
				tna.next_url, tna.view_count, tna.avg_reading_rate, tna.avg_time_spent, p.title, p.description, p.image, p.section, p.sub_section
			ORDER BY 
				engagement_score DESC
			LIMIT $4;
		`
		args = append(args, brand.Name, leadUuid, url, numResultsInt)

		// Execute the SQL query
		rows, err := db.Query(query, args...)
		if err != nil {
			log.Println(err.Error())
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Prepare the result set
		var articles []struct {
			URL                       string  `json:"url"`
			Title                     string  `json:"title"`
			Description               string  `json:"description"`
			Image                     *string `json:"image"`
			Section                   string  `json:"section"`
			SubSection                *string `json:"sub_section"`
			ViewCount                 int     `json:"view_count"`
			AvgReadingRate            float64 `json:"avg_reading_rate"`
			AvgTimeSpent              float64 `json:"avg_time_spent"`
			LeadArticlesInSameSection int     `json:"lead_articles_in_same_section"`
			EngagementScore           int     `json:"engagement_score"`
		}

		// Process the result set
		for rows.Next() {
			var article struct {
				URL                       string  `json:"url"`
				Title                     string  `json:"title"`
				Description               string  `json:"description"`
				Image                     *string `json:"image"`
				Section                   string  `json:"section"`
				SubSection                *string `json:"sub_section"`
				ViewCount                 int     `json:"view_count"`
				AvgReadingRate            float64 `json:"avg_reading_rate"`
				AvgTimeSpent              float64 `json:"avg_time_spent"`
				LeadArticlesInSameSection int     `json:"lead_articles_in_same_section"`
				EngagementScore           int     `json:"engagement_score"`
			}
			err := rows.Scan(&article.URL, &article.Title, &article.Description, &article.Image, &article.Section, &article.SubSection, &article.ViewCount, &article.AvgReadingRate, &article.AvgTimeSpent, &article.LeadArticlesInSameSection, &article.EngagementScore)
			if err != nil {
				log.Println(err.Error())
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}
			articles = append(articles, article)
		}

		// Convert the result to JSON
		responseData, err = json.Marshal(articles)
		if err != nil {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}

	// Store the result in Redis cache with a TTL of 1 seconds
	redisClient.Set(ctx, cacheKey, responseData, 1*time.Second)

	// Respond with JSON
	w.Header().Set("Content-Type", "application/json")
	w.Write(responseData)
}

// getLeadEngagementScore retrieves the engagement score for a specific lead with Redis caching
func getLeadEngagementScore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Extract host from the request's Host header
	host := r.Host
	if host == "" {
		http.Error(w, "Host header is required", http.StatusBadRequest)
		return
	}

	// Get the brand name
	brand, err := getBrandFromHost(host)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting brand: %v", err), http.StatusInternalServerError)
		return
	}

	// Extract lead_uuid from query parameters
	leadUUID := r.URL.Query().Get("lead_uuid")
	if leadUUID == "" {
		http.Error(w, "lead_uuid is required", http.StatusBadRequest)
		return
	}

	// Try to get cached data from Redis
	cacheKey := fmt.Sprintf("lead_engagement_score:%s:%s", brand.Name, leadUUID)
	cachedData, err := redisClient.Get(ctx, cacheKey).Result()
	if err == nil {
		// Return cached data if available
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(cachedData))
		return
	}

	// Define the SQL query
	query := `
		WITH monthly_metrics AS (
			SELECT
				SUM(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '90 DAYS' 
						AND calculation_period < NOW() - INTERVAL '60 DAYS' 
					THEN view_count 
					ELSE 0 
				END) AS views_month_1,
				SUM(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '60 DAYS' 
						AND calculation_period < NOW() - INTERVAL '30 DAYS' 
					THEN view_count 
					ELSE 0 
				END) AS views_month_2,
				SUM(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '30 DAYS' 
						AND calculation_period <= NOW() 
					THEN view_count 
					ELSE 0 
				END) AS views_month_3,

				ROUND(AVG(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '90 DAYS' 
						AND calculation_period < NOW() - INTERVAL '60 DAYS' 
					THEN avg_time_spent 
					ELSE 0 
				END), 2) AS avg_time_spent_month_1,
				ROUND(AVG(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '60 DAYS' 
						AND calculation_period < NOW() - INTERVAL '30 DAYS' 
					THEN avg_time_spent 
					ELSE 0 
				END), 2) AS avg_time_spent_month_2,
				ROUND(AVG(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '30 DAYS' 
						AND calculation_period <= NOW() 
					THEN avg_time_spent 
					ELSE 0 
				END), 2) AS avg_time_spent_month_3,

				ROUND(AVG(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '90 DAYS' 
						AND calculation_period < NOW() - INTERVAL '60 DAYS' 
					THEN avg_reading_rate 
					ELSE 0 
				END), 2) AS avg_reading_rate_month_1,
				ROUND(AVG(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '60 DAYS' 
						AND calculation_period < NOW() - INTERVAL '30 DAYS' 
					THEN avg_reading_rate 
					ELSE 0 
				END), 2) AS avg_reading_rate_month_2,
				ROUND(AVG(CASE 
					WHEN calculation_period >= NOW() - INTERVAL '30 DAYS' 
						AND calculation_period <= NOW() 
					THEN avg_reading_rate 
					ELSE 0 
				END), 2) AS avg_reading_rate_month_3
			FROM 
				lead_engagement_metrics
			WHERE 
				brand = $1
				AND lead_uuid = $2
				AND calculation_period >= NOW() - INTERVAL '90 DAYS'
			GROUP BY 
				lead_uuid
		)
		SELECT 
			u.is_subscriber AS user_is_subscriber,
			views_month_1,
			views_month_2,
			views_month_3,
			avg_time_spent_month_1,
			avg_time_spent_month_2,
			avg_time_spent_month_3,
			avg_reading_rate_month_1,
			avg_reading_rate_month_2,
			avg_reading_rate_month_3,
			CASE
				WHEN views_month_1 = 0 AND views_month_2 = 0 AND views_month_3 >= 0 THEN 0
				WHEN views_month_2 = 0 AND views_month_3 = 0 THEN -1
				ELSE
					LEAST(
						GREATEST(
							ROUND(
								CAST((
									(0.2 * (views_month_2 - views_month_1)) + 
									(0.5 * (views_month_3 - views_month_2)) +
									(0.1 * (avg_time_spent_month_2 - avg_time_spent_month_1)) +
									(0.3 * (avg_time_spent_month_3 - avg_time_spent_month_2)) +
									(0.1 * (avg_reading_rate_month_2 - avg_reading_rate_month_1)) + 
									(0.3 * (avg_reading_rate_month_3 - avg_reading_rate_month_2))
								) AS numeric) / NULLIF((views_month_3 + views_month_2 + views_month_1), 0)
							, 2)
						, -1)
					, 1)
			END AS score
		FROM 
			monthly_metrics
		LEFT JOIN 
			"user" u ON u.lead_uuid = $2 AND u.brand = $1
		LIMIT 1;
	`

	// Execute the query
	row := db.QueryRow(query, brand.Name, leadUUID)

	// Define a struct to hold the result
	var score struct {
		UserIsSubscriber     *bool   `json:"user_is_subscriber"`
		ViewsMonth1          int     `json:"views_month_1"`
		ViewsMonth2          int     `json:"views_month_2"`
		ViewsMonth3          int     `json:"views_month_3"`
		AvgTimeSpentMonth1   float64 `json:"avg_time_spent_month_1"`
		AvgTimeSpentMonth2   float64 `json:"avg_time_spent_month_2"`
		AvgTimeSpentMonth3   float64 `json:"avg_time_spent_month_3"`
		AvgReadingRateMonth1 float64 `json:"avg_reading_rate_month_1"`
		AvgReadingRateMonth2 float64 `json:"avg_reading_rate_month_2"`
		AvgReadingRateMonth3 float64 `json:"avg_reading_rate_month_3"`
		Score                float64 `json:"score"`
	}

	// Scan the result into the struct
	err = row.Scan(
		&score.UserIsSubscriber,
		&score.ViewsMonth1,
		&score.ViewsMonth2,
		&score.ViewsMonth3,
		&score.AvgTimeSpentMonth1,
		&score.AvgTimeSpentMonth2,
		&score.AvgTimeSpentMonth3,
		&score.AvgReadingRateMonth1,
		&score.AvgReadingRateMonth2,
		&score.AvgReadingRateMonth3,
		&score.Score,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "No score found for the given lead_uuid", http.StatusNotFound)
		} else {
			http.Error(w, "Failed to retrieve score: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Convert the result to JSON
	responseData, err := json.Marshal(score)
	if err != nil {
		http.Error(w, "Failed to marshal JSON", http.StatusInternalServerError)
		return
	}

	// Set the response header and write the JSON response
	w.Header().Set("Content-Type", "application/json")
	w.Write(responseData)

	// Cache the result in Redis for 1 minute
	err = redisClient.Set(ctx, cacheKey, string(responseData), 1*time.Minute).Err()
	if err != nil {
		fmt.Printf("Failed to cache result: %v\n", err)
	}
}

// ServeJS serves the JavaScript file for the Weather library
func ServeJSLibrary(w http.ResponseWriter, r *http.Request) {
	// Create a new minificator
	m := minify.New()

	jsMinifier := &js.Minifier{
		KeepVarNames: false,
	}

	m.Add("text/javascript", jsMinifier)

	// Read the JavaScript file
	file, err := os.Open("assets/javascript/weather.js")
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	defer file.Close()

	// Minify the JavaScript file
	w.Header().Set("Content-Type", "text/javascript")
	err = m.Minify("text/javascript", w, file)
	if err != nil {
		http.Error(w, fmt.Sprintf("Minification failed: %v", err), http.StatusInternalServerError)
		return
	}
}

// ServeHTML serves the HTML page for the test home
func ServeTestHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "assets/html/test/index.html")
}

// ServeHTML serves the HTML page for the test article 1
func ServeTestArticle1(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "assets/html/test/article1.html")
}

// ServeHTML serves the HTML page for the test article 2
func ServeTestArticle2(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "assets/html/test/article2.html")
}

// Helper function to generate a new UUID
func generateUUID() string {
	newUUID := uuid.New()
	return newUUID.String()
}

// getClientIP returns the client's IP address from the request
func getClientIP(r *http.Request) string {
	// Try to get the IP from the X-Forwarded-For header
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		// X-Forwarded-For can contain multiple IPs, take the first one
		ips := strings.Split(forwarded, ",")
		clientIP := strings.TrimSpace(ips[0])
		return clientIP
	}

	// Otherwise, try to get the IP from the X-Real-IP header
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		return realIP
	}

	// If none found, use the IP from RemoteAddr field
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr // Return RemoteAddr directly in case of an error
	}

	return ip
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

	// Initialize Redis client
	redisClient = redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	// Verify Redis connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to connect to Redis: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to Redis")

	db, err = sql.Open("postgres", os.Getenv("POSTGRES_DSN"))
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to connect to PostgreSQL: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to PostgreSQL")

	psClient, err = pubsub.NewClient(ctx, os.Getenv("GCP_PROJECT_ID"), option.WithCredentialsFile(os.Getenv("GCP_CREDENTIALS_FILE")))
	if err != nil {
		logger.LogFatal("[SYSTEM] Failed to create Pub/Sub client: %v", err)
	}
	logger.LogInfo("[SYSTEM] Connected to PubSub")
}

// Main function to start the server
func main() {
	// Health check
	http.HandleFunc("/health", healthCheckHandler)

	// Collectors
	http.HandleFunc("/collect/v1/page-data", collectPageDataHandler)
	http.HandleFunc("/collect/v1/user-data", collectUserDataHandler)
	http.HandleFunc("/collect/v1/lead-event", collectLeadEventDataHandler)

	// Leads
	http.HandleFunc("/api/v1/lead/engagement-score", getLeadEngagementScore)

	// Articles
	http.HandleFunc("/api/v1/article/metrics", getArticleMetrics)
	http.HandleFunc("/api/v1/articles/top-articles", getTopArticles)
	http.HandleFunc("/api/v1/article/top-next-articles", getArticleTopNextArticles)
	http.HandleFunc("/api/v1/article/content-based-articles", getArticleContentBasedArticlesHandler)

	// Javascript SDK
	http.HandleFunc("/weather.js", ServeJSLibrary)

	// Exemple pages
	http.HandleFunc("/test", ServeTestHome)
	http.HandleFunc("/test/article-1.html", ServeTestArticle1)
	http.HandleFunc("/test/article-2.html", ServeTestArticle2)

	// Use the PORT environment variable or default to 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	logger.LogInfo("[SYSTEM] Server started on port :%s", port)
	logger.LogFatal("[SYSTEM] " + http.ListenAndServe(":"+port, nil).Error())
}
