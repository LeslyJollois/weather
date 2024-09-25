package main

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/evaluation"
	"github.com/sjwhitworth/golearn/knn"
)

// Define weights for each metric
var attributesWeight = map[string]float64{
	"article_count": 2.0, // Double weight for article_count
	"reading_rate":  2.0, // Double weight for reading_rate
	"time_spent":    0.5, // Half weight for time_spent
}

var categories = []string{
	"Fashion",       // Clothing, accessories, trends
	"Health",        // Wellness, fitness, medical topics
	"Technology",    // Gadgets, IT, software, hardware
	"Sports",        // Physical activities, sports events, teams
	"Finance",       // Financial advice, investments, banking
	"Entertainment", // Movies, TV shows, music, pop culture
	"Travel",        // Destinations, travel tips, tourism
	"Food",          // Cooking, recipes, dining out
	"Education",     // Schooling, learning, online courses
	"Automotive",    // Cars, motorcycles, auto industry
	"Real Estate",   // Housing, property investments, real estate market
	"Parenting",     // Parenting tips, child education, family life
	"Politics",      // Political news, policy analysis, elections
	"Environment",   // Sustainability, climate change, ecological topics
	"Science",       // Research, discoveries, natural sciences
	"Gaming",        // Video games, eSports, game reviews
	"Literature",    // Books, literary reviews, authors
	"Beauty",        // Skincare, makeup, personal wellness
	"Home & Garden", // Home decor, gardening, DIY home projects
	"Relationships", // Relationship advice, dating, psychology
	"Pets",          // Pet care, advice for pet owners, animal welfare
	"History",       // Historical events, history studies, biographies
	"Art & Culture", // Visual arts, cultural heritage, museums
	"DIY & Crafts",  // Handicrafts, DIY projects, creative crafts
}

var categoryEncoding = map[string]float64{
	"Fashion":       0.0,
	"Health":        1.0,
	"Technology":    2.0,
	"Sports":        3.0,
	"Finance":       4.0,
	"Entertainment": 5.0,
	"Travel":        6.0,
	"Food":          7.0,
	"Education":     8.0,
	"Automotive":    9.0,
	"Real Estate":   10.0,
	"Parenting":     11.0,
	"Politics":      12.0,
	"Environment":   13.0,
	"Science":       14.0,
	"Gaming":        15.0,
	"Literature":    16.0,
	"Beauty":        17.0,
	"Home & Garden": 18.0,
	"Relationships": 19.0,
	"Pets":          20.0,
	"History":       21.0,
	"Art & Culture": 22.0,
	"DIY & Crafts":  23.0,
}

var genderEncoding = map[string]float64{
	"Male":   0.0,
	"Female": 1.0,
}

var ageGroupEncoding = map[string]float64{
	"0-17":  0.0, // Minors
	"18-25": 1.0, // Young adults (younger)
	"26-35": 2.0, // Young adults (mid-career)
	"36-45": 3.0, // Middle-aged adults
	"46-54": 4.0, // Older adults
	"55-64": 5.0, // Pre-retirees or active seniors
	"65-74": 6.0, // Retirees
	"75+":   7.0, // Elderly
}

var intellectualLevelEncoding = map[string]float64{
	"Low":       0.0,
	"Medium":    1.0,
	"High":      2.0,
	"Very High": 3.0,
}

var deviceEncoding = map[string]float64{
	"Smartphone": 0.0,
	"Tablet":     1.0,
	"Computer":   2.0,
}

// Define categories associated with specific gender probabilities
var categoryGenderMap = map[string][]struct {
	gender string  // Gender associated with the category
	weight float64 // Probability that the category is associated with this gender
}{
	"Fashion": {
		{"Female", 0.8},
		{"Male", 0.2},
	},
	"Health": {
		{"Female", 0.7},
		{"Male", 0.3},
	},
	"Technology": {
		{"Male", 0.8},
		{"Female", 0.2},
	},
	"Sports": {
		{"Male", 0.7},
		{"Female", 0.3},
	},
	"Finance": {
		{"Male", 0.8},
		{"Female", 0.2},
	},
	"Entertainment": {
		{"Female", 0.5},
		{"Male", 0.5},
	},
	"Travel": {
		{"Female", 0.6},
		{"Male", 0.4},
	},
	"Food": {
		{"Female", 0.7},
		{"Male", 0.3},
	},
	"Education": {
		{"Female", 0.6},
		{"Male", 0.4},
	},
	"Automotive": {
		{"Male", 0.9},
		{"Female", 0.1},
	},
	"Real Estate": {
		{"Male", 0.7},
		{"Female", 0.3},
	},
	"Parenting": {
		{"Female", 0.8},
		{"Male", 0.2},
	},
	"Politics": {
		{"Male", 0.6},
		{"Female", 0.4},
	},
	"Environment": {
		{"Female", 0.5},
		{"Male", 0.5},
	},
	"Science": {
		{"Male", 0.7},
		{"Female", 0.3},
	},
	"Gaming": {
		{"Male", 0.9},
		{"Female", 0.1},
	},
	"Literature": {
		{"Female", 0.6},
		{"Male", 0.4},
	},
	"Beauty": {
		{"Female", 0.9},
		{"Male", 0.1},
	},
	"Home & Garden": {
		{"Female", 0.6},
		{"Male", 0.4},
	},
	"Relationships": {
		{"Female", 0.7},
		{"Male", 0.3},
	},
	"Pets": {
		{"Female", 0.6},
		{"Male", 0.4},
	},
	"History": {
		{"Male", 0.6},
		{"Female", 0.4},
	},
	"Art & Culture": {
		{"Female", 0.7},
		{"Male", 0.3},
	},
	"DIY & Crafts": {
		{"Female", 0.7},
		{"Male", 0.3},
	},
}

// Define categories associated with specific age groups probabilities
var categoryAgeGroupMap = map[string][]struct {
	ageGroup string  // Age group associated with the category
	weight   float64 // Probability that the category is associated with this age group
}{
	"Fashion": {
		{"18-25", 0.6},
		{"26-35", 0.3},
		{"36-45", 0.1},
	},
	"Health": {
		{"26-35", 0.2},
		{"36-45", 0.3},
		{"46-54", 0.2},
		{"55-64", 0.2},
		{"65-74", 0.1},
	},
	"Technology": {
		{"18-25", 0.4},
		{"26-35", 0.4},
		{"36-45", 0.2},
	},
	"Sports": {
		{"18-25", 0.5},
		{"26-35", 0.3},
		{"36-45", 0.2},
	},
	"Finance": {
		{"36-45", 0.3},
		{"46-54", 0.3},
		{"55-64", 0.2},
		{"65-74", 0.2},
	},
	"Entertainment": {
		{"18-25", 0.5},
		{"26-35", 0.3},
		{"36-45", 0.2},
	},
	"Travel": {
		{"18-25", 0.2},
		{"26-35", 0.4},
		{"36-45", 0.3},
		{"46-54", 0.1},
	},
	"Food": {
		{"18-25", 0.3},
		{"26-35", 0.4},
		{"36-45", 0.2},
		{"46-54", 0.1},
	},
	"Education": {
		{"18-25", 0.4},
		{"26-35", 0.3},
		{"36-45", 0.2},
		{"46-54", 0.1},
	},
	"Automotive": {
		{"26-35", 0.2},
		{"36-45", 0.4},
		{"46-54", 0.3},
		{"55-64", 0.1},
	},
	"Real Estate": {
		{"36-45", 0.2},
		{"46-54", 0.3},
		{"55-64", 0.3},
		{"65-74", 0.2},
	},
	"Parenting": {
		{"26-35", 0.1},
		{"36-45", 0.4},
		{"46-54", 0.4},
		{"55-64", 0.1},
	},
	"Politics": {
		{"36-45", 0.2},
		{"46-54", 0.3},
		{"55-64", 0.3},
		{"65-74", 0.2},
	},
	"Environment": {
		{"26-35", 0.3},
		{"36-45", 0.4},
		{"46-54", 0.2},
		{"55-64", 0.1},
	},
	"Science": {
		{"18-25", 0.3},
		{"26-35", 0.4},
		{"36-45", 0.2},
		{"46-54", 0.1},
	},
	"Gaming": {
		{"18-25", 0.7},
		{"26-35", 0.2},
		{"36-45", 0.1},
	},
	"Literature": {
		{"18-25", 0.2},
		{"26-35", 0.3},
		{"36-45", 0.3},
		{"46-54", 0.2},
	},
	"Beauty": {
		{"18-25", 0.6},
		{"26-35", 0.3},
		{"36-45", 0.1},
	},
	"Home & Garden": {
		{"36-45", 0.3},
		{"46-54", 0.3},
		{"55-64", 0.3},
		{"65-74", 0.1},
	},
	"Relationships": {
		{"18-25", 0.4},
		{"26-35", 0.4},
		{"36-45", 0.2},
	},
	"Pets": {
		{"18-25", 0.3},
		{"26-35", 0.4},
		{"36-45", 0.2},
		{"46-54", 0.1},
	},
	"History": {
		{"26-35", 0.2},
		{"36-45", 0.3},
		{"46-54", 0.3},
		{"55-64", 0.2},
	},
	"Art & Culture": {
		{"18-25", 0.2},
		{"26-35", 0.3},
		{"36-45", 0.4},
		{"46-54", 0.1},
	},
	"DIY & Crafts": {
		{"26-35", 0.3},
		{"36-45", 0.4},
		{"46-54", 0.2},
		{"55-64", 0.1},
	},
}

// Define categories associated with specific intellectual levels
var categoryIntellectualLevelMap = map[string][]struct {
	level  string  // Intellectual level associated with the category
	weight float64 // Weight indicating the likelihood of association
}{
	"Fashion": {
		{"Low", 0.7},
		{"Medium", 0.3},
	},
	"Health": {
		{"Medium", 0.5},
		{"High", 0.5},
	},
	"Technology": {
		{"High", 0.6},
		{"Medium", 0.4},
	},
	"Sports": {
		{"Low", 0.6},
		{"Medium", 0.4},
	},
	"Finance": {
		{"High", 0.7},
		{"Medium", 0.3},
	},
	"Entertainment": {
		{"Low", 0.5},
		{"Medium", 0.5},
	},
	"Travel": {
		{"Medium", 0.6},
		{"Low", 0.4},
	},
	"Food": {
		{"Low", 0.4},
		{"Medium", 0.6},
	},
	"Education": {
		{"High", 0.8},
		{"Medium", 0.2},
	},
	"Automotive": {
		{"Medium", 0.6},
		{"Low", 0.4},
	},
	"Real Estate": {
		{"High", 0.6},
		{"Medium", 0.4},
	},
	"Parenting": {
		{"Low", 0.6},
		{"Medium", 0.4},
	},
	"Politics": {
		{"High", 0.7},
		{"Medium", 0.3},
	},
	"Environment": {
		{"Medium", 0.6},
		{"Low", 0.4},
	},
	"Science": {
		{"Very High", 0.8},
		{"High", 0.2},
	},
	"Gaming": {
		{"Low", 0.6},
		{"Medium", 0.4},
	},
	"Literature": {
		{"High", 0.7},
		{"Medium", 0.3},
	},
	"Beauty": {
		{"Low", 0.6},
		{"Medium", 0.4},
	},
	"Home & Garden": {
		{"Low", 0.5},
		{"Medium", 0.5},
	},
	"Relationships": {
		{"Low", 0.5},
		{"Medium", 0.5},
	},
	"Pets": {
		{"Low", 0.5},
		{"Medium", 0.5},
	},
	"History": {
		{"High", 0.6},
		{"Medium", 0.4},
	},
	"Art & Culture": {
		{"High", 0.6},
		{"Medium", 0.4},
	},
	"DIY & Crafts": {
		{"Low", 0.5},
		{"Medium", 0.5},
	},
}

// Define devices associated with specific age groups and their weights
var ageDeviceMap = map[string][]struct {
	device string
	weight float64
}{
	"0-17": {
		{"Smartphone", 0.7},
		{"Tablet", 0.2},
		{"Computer", 0.1},
	},
	"18-25": {
		{"Smartphone", 0.8},
		{"Tablet", 0.1},
		{"Computer", 0.1},
	},
	"26-35": {
		{"Smartphone", 0.6},
		{"Tablet", 0.15},
		{"Computer", 0.25},
	},
	"36-45": {
		{"Smartphone", 0.5},
		{"Tablet", 0.2},
		{"Computer", 0.3},
	},
	"46-54": {
		{"Smartphone", 0.4},
		{"Tablet", 0.25},
		{"Computer", 0.35},
	},
	"55-64": {
		{"Smartphone", 0.35},
		{"Tablet", 0.3},
		{"Computer", 0.35},
	},
	"65-74": {
		{"Smartphone", 0.25},
		{"Tablet", 0.35},
		{"Computer", 0.4},
	},
	"75+": {
		{"Smartphone", 0.2},
		{"Tablet", 0.4},
		{"Computer", 0.4},
	},
}

const PREDICT_GENDER = 1
const PREDICT_AGE_GROUP = 2
const PREDICT_INTELLECTUAL_LEVEL = 3

// Function to select categories based on gender with a minimum average weight
func selectCategoriesFromGender(rng *rand.Rand, gender string, numResults int, minWeight float64) ([]string, float64) {
	selectedCategories := []string{}
	totalWeight := 0.0

	// Loop until the desired number of categories with sufficient average weight are selected
	for len(selectedCategories) < numResults {
		for category, genders := range categoryGenderMap {
			for _, item := range genders {
				// Check if the gender matches the desired gender
				if item.gender == gender {
					// Generate a random value to decide if this category is selected
					if rng.Float64() <= item.weight {
						selectedCategories = append(selectedCategories, category)
						totalWeight += item.weight

						// Calculate the current average weight
						averageWeight := totalWeight / float64(len(selectedCategories))

						// If we've reached the desired number of results and average weight, return
						if len(selectedCategories) >= numResults && averageWeight >= minWeight {
							return selectedCategories, averageWeight
						}
					}
				}
			}
		}

		// Reset if the conditions are not met after one pass over the data
		if len(selectedCategories) < numResults || (totalWeight/float64(len(selectedCategories)) < minWeight) {
			selectedCategories = []string{}
			totalWeight = 0.0
		}
	}

	// Calculate and return the final average weight
	averageWeight := totalWeight / float64(len(selectedCategories))
	return selectedCategories, averageWeight
}

// Function to select the average gender from multiple categories based on their distribution
func selectAverageGenderFromCategories(rng *rand.Rand, categories []string) (string, float64) {
	genderWeights := make(map[string]float64)
	totalWeight := 0.0

	// Aggregate weights for each gender across categories
	for _, category := range categories {
		for _, item := range categoryGenderMap[category] {
			genderWeights[item.gender] += item.weight
			totalWeight += item.weight
		}
	}

	// Normalize weights to find the average gender
	cumulativeWeight := 0.0
	randomValue := rng.Float64() * totalWeight

	for gender, weight := range genderWeights {
		cumulativeWeight += weight
		if randomValue <= cumulativeWeight {
			return gender, weight / totalWeight // Return the average weight
		}
	}

	// Fallback to last gender in case of any rounding issues
	var lastGender string
	var lastWeight float64
	for gender, weight := range genderWeights {
		lastGender = gender
		lastWeight = weight / totalWeight
	}

	return lastGender, lastWeight
}

// Function to select the average age group from multiple categories
func selectAverageAgeGroupFromCategories(rng *rand.Rand, categories []string) (string, float64) {
	ageGroupWeights := make(map[string]float64)
	totalWeight := 0.0

	// Aggregate weights for each age group across categories
	for _, category := range categories {
		for _, item := range categoryAgeGroupMap[category] {
			ageGroupWeights[item.ageGroup] += item.weight
			totalWeight += item.weight
		}
	}

	// Normalize weights to find the average age group
	cumulativeWeight := 0.0
	randomValue := rng.Float64() * totalWeight

	for ageGroup, weight := range ageGroupWeights {
		cumulativeWeight += weight
		if randomValue <= cumulativeWeight {
			return ageGroup, weight / totalWeight // Return the average weight
		}
	}

	// Fallback to last age group in case of any rounding issues
	var lastAgeGroup string
	var lastWeight float64
	for ageGroup, weight := range ageGroupWeights {
		lastAgeGroup = ageGroup
		lastWeight = weight / totalWeight
	}

	return lastAgeGroup, lastWeight
}

// Function to select the average intellectual level from multiple categories based on their distribution
func selectAverageIntellectualLevelFromCategories(rng *rand.Rand, categories []string) (string, float64) {
	// Initialize a map to accumulate weights for each intellectual level
	levelWeights := make(map[string]float64)
	totalWeight := 0.0

	// Aggregate weights for each intellectual level across categories
	for _, category := range categories {
		for _, item := range categoryIntellectualLevelMap[category] {
			levelWeights[item.level] += item.weight
			totalWeight += item.weight
		}
	}

	// Generate a random value for cumulative weight selection
	randomValue := rng.Float64() * totalWeight
	cumulativeWeight := 0.0

	// Select the intellectual level based on cumulative distribution
	for level, weight := range levelWeights {
		cumulativeWeight += weight
		if randomValue <= cumulativeWeight {
			return level, weight / totalWeight // Return normalized weight
		}
	}

	// Fallback to the last intellectual level in case of rounding issues
	var lastLevel string
	var lastWeight float64
	for level, weight := range levelWeights {
		lastLevel = level
		lastWeight = weight / totalWeight
	}

	return lastLevel, lastWeight
}

// Function to select a random device based on the given age group and device weights
func selectDeviceFromAgeGroup(rng *rand.Rand, ageGroup string) (string, float64) {
	// Get the device options and their weights for the given age group
	devices := ageDeviceMap[ageGroup]

	// Calculate the cumulative weight to normalize
	var cumulativeWeight float64
	for _, device := range devices {
		cumulativeWeight += device.weight
	}

	// Generate a random number between 0 and the cumulative weight
	randomWeight := rng.Float64() * cumulativeWeight

	// Select a device based on the random weight
	var sum float64
	for _, device := range devices {
		sum += device.weight
		if randomWeight <= sum {
			return device.device, device.weight
		}
	}

	// Default to the first device if something goes wrong
	return devices[0].device, devices[0].weight
}

// Function to determine the multiplier based on the age group
func getAgeGroupMultiplier(ageGroup string) float64 {
	switch ageGroup {
	case "0-17":
		return 0.7
	case "18-25":
		return 1.0
	case "26-35":
		return 1.2
	case "36-45":
		return 1.5
	case "46-54":
		return 1.7
	case "55-64":
		return 1.9
	case "65-74", "75+":
		return 2.0
	default:
		return 1.0
	}
}

// Function to determine the multiplier based on the age group
func getIntellectualLevelMultiplier(ageGroup string) float64 {
	switch ageGroup {
	case "Low":
		return 0.7
	case "Medium":
		return 1.0
	case "High":
		return 1.2
	case "Very High":
		return 1.5
	default:
		return 1.0
	}
}

// Function to generate random data for 1000 users
func generateUserData() []map[string]interface{} {
	// Use a fixed seed for reproducible results on each call
	rng := rand.New(rand.NewSource(42)) // Seed fixed at 42 for reproducibility

	userData := make([]map[string]interface{}, 1000)

	for i := 0; i < 1000; i++ {
		// Alternate genders with a probabilistic approach for more realism
		gender := "Male"
		if rng.Float64() < 0.5 {
			gender = "Female"
		}

		// Select 3 categories based on the user's gender
		articleCategories, genderWeight := selectCategoriesFromGender(rng, gender, 3, 0.8)

		// Define shared values for age, engagement, and intellectual level across articles
		ageGroup, ageGroupWeight := selectAverageAgeGroupFromCategories(rng, articleCategories)
		intellectualLevel, intellectualLevelWeight := selectAverageIntellectualLevelFromCategories(rng, articleCategories)
		device, _ := selectDeviceFromAgeGroup(rng, ageGroup)

		// Age group multiplier for age-based adjustments
		ageGroupMultiplier := getAgeGroupMultiplier(ageGroup) * ageGroupWeight

		// Intellectual level multiplier for reading rate and time spent adjustments
		intellectualLevelMultiplier := getIntellectualLevelMultiplier(intellectualLevel) * intellectualLevelWeight

		// Define engagement levels based on gender, age, and intellectual level
		var articleCount int
		var readingRate, timeSpent float64

		// Apply genderWeight and intellectual level for more realistic variation
		if gender == "Female" && genderWeight > 0.5 {
			articleCount = int(rng.NormFloat64()*2 + 10)                                                  // Average 10 articles
			readingRate = (75.0 * ageGroupMultiplier * intellectualLevelMultiplier) + rng.NormFloat64()*5 // Around 75.0, adjusted by age and intellect
			timeSpent = (90.0 * ageGroupMultiplier * intellectualLevelMultiplier) + rng.NormFloat64()*10  // Around 90.0, adjusted by age and intellect
		} else {
			articleCount = int(rng.NormFloat64()*2 + 7)                                                   // Average 7 articles
			readingRate = (50.0 * ageGroupMultiplier * intellectualLevelMultiplier) + rng.NormFloat64()*5 // Around 50.0, adjusted by age and intellect
			timeSpent = (70.0 * ageGroupMultiplier * intellectualLevelMultiplier) + rng.NormFloat64()*10  // Around 70.0, adjusted by age and intellect
		}

		user := map[string]interface{}{
			"user_id":            fmt.Sprintf("%d", i+1),
			"article_category_1": articleCategories[0],
			"article_category_2": articleCategories[1],
			"article_category_3": articleCategories[2],
			"article_count_1":    articleCount,
			"article_count_2":    articleCount,
			"article_count_3":    articleCount,
			"reading_rate_1":     readingRate,
			"reading_rate_2":     readingRate,
			"reading_rate_3":     readingRate,
			"time_spent_1":       timeSpent,
			"time_spent_2":       timeSpent,
			"time_spent_3":       timeSpent,
			"device":             device,
			"gender":             gender,
			"age_group":          ageGroup,
			"intellectual_level": intellectualLevel,
		}

		userData[i] = user
	}

	return userData
}

// Encode category as a numeric value
func encodeCategory(category string) float64 {
	return categoryEncoding[category]
}

// Encode gender as a numeric value (0 for Male, 1 for Female)
func encodeGender(gender string) float64 {
	return genderEncoding[gender]
}

// Encode age group as a numeric value
func encodeAgeGroup(ageGroup string) float64 {
	return ageGroupEncoding[ageGroup]
}

// Encode age group as a numeric value
func encodeIntellectualLevel(intellectualLevel string) float64 {
	return intellectualLevelEncoding[intellectualLevel]
}

// Encode device as a numeric value
func encodeDevice(device string) float64 {
	return deviceEncoding[device]
}

// Encode gender as a numeric value (0 for Male, 1 for Female)
func decodeGender(prediction float64) string {
	for gender, score := range genderEncoding {
		if score == prediction {
			return gender
		}
	}
	return "Unknown"
}

func decodeAgeGroup(prediction float64) string {
	for ageGroup, value := range ageGroupEncoding {
		if value == prediction {
			return ageGroup
		}
	}
	return "Unknown"
}

func decodeIntellectualLevel(prediction float64) string {
	for intellectualLevel, value := range intellectualLevelEncoding {
		if value == prediction {
			return intellectualLevel
		}
	}
	return "Unknown"
}

func createDataset(userData []map[string]interface{}, classToPredict int) *base.DenseInstances {
	// Create numeric attributes
	articleCategory1Attr := base.NewFloatAttribute("article_category_1")
	articleCategory2Attr := base.NewFloatAttribute("article_category_2")
	articleCategory3Attr := base.NewFloatAttribute("article_category_3")
	articleCount1Attr := base.NewFloatAttribute("article_count_1")
	articleCount2Attr := base.NewFloatAttribute("article_count_2")
	articleCount3Attr := base.NewFloatAttribute("article_count_3")
	readingRate1Attr := base.NewFloatAttribute("reading_rate_1")
	readingRate2Attr := base.NewFloatAttribute("reading_rate_2")
	readingRate3Attr := base.NewFloatAttribute("reading_rate_3")
	timeSpent1Attr := base.NewFloatAttribute("time_spent_1")
	timeSpent2Attr := base.NewFloatAttribute("time_spent_2")
	timeSpent3Attr := base.NewFloatAttribute("time_spent_3")
	deviceAttr := base.NewFloatAttribute("device")

	var classToPredictAttr *base.FloatAttribute
	if classToPredict == PREDICT_GENDER {
		classToPredictAttr = base.NewFloatAttribute("gender")
	} else if classToPredict == PREDICT_AGE_GROUP {
		classToPredictAttr = base.NewFloatAttribute("age_group")
	} else if classToPredict == PREDICT_INTELLECTUAL_LEVEL {
		classToPredictAttr = base.NewFloatAttribute("intellectual_level")
	}

	// Create a dataset with 2 attributes (article category and gender)
	dataset := base.NewDenseInstances()
	articleCategory1Spec := dataset.AddAttribute(articleCategory1Attr)
	articleCategory2Spec := dataset.AddAttribute(articleCategory2Attr)
	articleCategory3Spec := dataset.AddAttribute(articleCategory3Attr)
	articleCount1Spec := dataset.AddAttribute(articleCount1Attr)
	articleCount2Spec := dataset.AddAttribute(articleCount2Attr)
	articleCount3Spec := dataset.AddAttribute(articleCount3Attr)
	readingRate1Spec := dataset.AddAttribute(readingRate1Attr)
	readingRate2Spec := dataset.AddAttribute(readingRate2Attr)
	readingRate3Spec := dataset.AddAttribute(readingRate3Attr)
	timeSpent1Spec := dataset.AddAttribute(timeSpent1Attr)
	timeSpent2Spec := dataset.AddAttribute(timeSpent2Attr)
	timeSpent3Spec := dataset.AddAttribute(timeSpent3Attr)
	deviceSpec := dataset.AddAttribute(deviceAttr)
	classToPredictSpec := dataset.AddAttribute(classToPredictAttr)
	dataset.AddClassAttribute(classToPredictAttr) // Class to predict

	// Allocate space for data points
	dataset.Extend(1000)

	// Fill dataset with the generated data
	for i, record := range userData {
		articleCategory1 := encodeCategory(record["article_category_1"].(string))
		articleCategory2 := encodeCategory(record["article_category_2"].(string))
		articleCategory3 := encodeCategory(record["article_category_3"].(string))
		articleCount1 := record["article_count_1"].(int)
		articleCount2 := record["article_count_2"].(int)
		articleCount3 := record["article_count_3"].(int)
		readingRate1 := record["reading_rate_1"].(float64)
		readingRate2 := record["reading_rate_2"].(float64)
		readingRate3 := record["reading_rate_3"].(float64)
		timeSpent1 := record["time_spent_1"].(float64)
		timeSpent2 := record["time_spent_2"].(float64)
		timeSpent3 := record["time_spent_3"].(float64)
		device := encodeDevice(record["device"].(string))

		var classToPredictValue float64
		if classToPredict == PREDICT_GENDER {
			classToPredictValue = encodeGender(record["gender"].(string))
		} else if classToPredict == PREDICT_AGE_GROUP {
			classToPredictValue = encodeAgeGroup(record["age_group"].(string))
		} else if classToPredict == PREDICT_INTELLECTUAL_LEVEL {
			classToPredictValue = encodeIntellectualLevel(record["intellectual_level"].(string))
		}

		// Set article category as feature and gender as class
		dataset.Set(articleCategory1Spec, i, base.PackFloatToBytes(articleCategory1))
		dataset.Set(articleCategory2Spec, i, base.PackFloatToBytes(articleCategory2))
		dataset.Set(articleCategory3Spec, i, base.PackFloatToBytes(articleCategory3))
		dataset.Set(articleCount1Spec, i, base.PackFloatToBytes(float64(articleCount1)*attributesWeight["article_count"]))
		dataset.Set(articleCount2Spec, i, base.PackFloatToBytes(float64(articleCount2)*attributesWeight["article_count"]))
		dataset.Set(articleCount3Spec, i, base.PackFloatToBytes(float64(articleCount3)*attributesWeight["article_count"]))
		dataset.Set(readingRate1Spec, i, base.PackFloatToBytes(readingRate1*attributesWeight["reading_rate"]))
		dataset.Set(readingRate2Spec, i, base.PackFloatToBytes(readingRate2*attributesWeight["reading_rate"]))
		dataset.Set(readingRate3Spec, i, base.PackFloatToBytes(readingRate3*attributesWeight["reading_rate"]))
		dataset.Set(timeSpent1Spec, i, base.PackFloatToBytes(timeSpent1*attributesWeight["time_spent"]))
		dataset.Set(timeSpent2Spec, i, base.PackFloatToBytes(timeSpent2*attributesWeight["time_spent"]))
		dataset.Set(timeSpent3Spec, i, base.PackFloatToBytes(timeSpent3*attributesWeight["time_spent"]))
		dataset.Set(deviceSpec, i, base.PackFloatToBytes(device))
		dataset.Set(classToPredictSpec, i, base.PackFloatToBytes(classToPredictValue)) // Class to predict
	}

	return dataset
}

func createSingleInstanceFromTrainData(articleCategories []string, articleCounts []int, readingRates []float64, timeSpents []float64, device string, classToPredict int) *base.DenseInstances {
	// Create numeric attributes
	articleCategory1Attr := base.NewFloatAttribute("article_category_1")
	articleCategory2Attr := base.NewFloatAttribute("article_category_2")
	articleCategory3Attr := base.NewFloatAttribute("article_category_3")
	articleCount1Attr := base.NewFloatAttribute("article_count_1")
	articleCount2Attr := base.NewFloatAttribute("article_count_2")
	articleCount3Attr := base.NewFloatAttribute("article_count_3")
	readingRate1Attr := base.NewFloatAttribute("reading_rate_1")
	readingRate2Attr := base.NewFloatAttribute("reading_rate_2")
	readingRate3Attr := base.NewFloatAttribute("reading_rate_3")
	timeSpent1Attr := base.NewFloatAttribute("time_spent_1")
	timeSpent2Attr := base.NewFloatAttribute("time_spent_2")
	timeSpent3Attr := base.NewFloatAttribute("time_spent_3")
	deviceAttr := base.NewFloatAttribute("device")

	var classToPredictAttr *base.FloatAttribute
	if classToPredict == PREDICT_GENDER {
		classToPredictAttr = base.NewFloatAttribute("gender")
	} else if classToPredict == PREDICT_AGE_GROUP {
		classToPredictAttr = base.NewFloatAttribute("age_group")
	} else if classToPredict == PREDICT_INTELLECTUAL_LEVEL {
		classToPredictAttr = base.NewFloatAttribute("intellectual_level")
	}

	// Create a dataset
	dataset := base.NewDenseInstances()
	articleCategory1Spec := dataset.AddAttribute(articleCategory1Attr)
	articleCategory2Spec := dataset.AddAttribute(articleCategory2Attr)
	articleCategory3Spec := dataset.AddAttribute(articleCategory3Attr)
	articleCount1Spec := dataset.AddAttribute(articleCount1Attr)
	articleCount2Spec := dataset.AddAttribute(articleCount2Attr)
	articleCount3Spec := dataset.AddAttribute(articleCount3Attr)
	readingRate1Spec := dataset.AddAttribute(readingRate1Attr)
	readingRate2Spec := dataset.AddAttribute(readingRate2Attr)
	readingRate3Spec := dataset.AddAttribute(readingRate3Attr)
	timeSpent1Spec := dataset.AddAttribute(timeSpent1Attr)
	timeSpent2Spec := dataset.AddAttribute(timeSpent2Attr)
	timeSpent3Spec := dataset.AddAttribute(timeSpent3Attr)
	deviceSpec := dataset.AddAttribute(deviceAttr)
	dataset.AddAttribute(classToPredictAttr)
	dataset.AddClassAttribute(classToPredictAttr) // Class to predict

	// Allocate space for data points
	dataset.Extend(1)

	// Fill dataset with the generated data
	encodedArticleCategory1 := encodeCategory(articleCategories[0])
	dataset.Set(articleCategory1Spec, 0, base.PackFloatToBytes(encodedArticleCategory1))

	if len(articleCategories) > 1 {
		encodedArticleCategory2 := encodeCategory(articleCategories[1])
		dataset.Set(articleCategory2Spec, 0, base.PackFloatToBytes(encodedArticleCategory2))
	}

	if len(articleCategories) > 2 {
		encodedArticleCategory3 := encodeCategory(articleCategories[2])
		dataset.Set(articleCategory3Spec, 0, base.PackFloatToBytes(encodedArticleCategory3))
	}

	encodedDevice := encodeDevice(device)

	// Set article category as feature and gender as class
	dataset.Set(articleCount1Spec, 0, base.PackFloatToBytes(float64(articleCounts[0])*attributesWeight["article_count"]))

	if len(articleCounts) > 1 {
		dataset.Set(articleCount2Spec, 0, base.PackFloatToBytes(float64(articleCounts[1])*attributesWeight["article_count"]))
	}

	if len(articleCounts) > 2 {
		dataset.Set(articleCount3Spec, 0, base.PackFloatToBytes(float64(articleCounts[2])*attributesWeight["article_count"]))
	}

	dataset.Set(readingRate1Spec, 0, base.PackFloatToBytes(readingRates[0]*attributesWeight["reading_rate"]))

	if len(readingRates) > 1 {
		dataset.Set(readingRate2Spec, 0, base.PackFloatToBytes(readingRates[1]*attributesWeight["reading_rate"]))
	}

	if len(readingRates) > 2 {
		dataset.Set(readingRate3Spec, 0, base.PackFloatToBytes(readingRates[2]*attributesWeight["reading_rate"]))
	}

	dataset.Set(timeSpent1Spec, 0, base.PackFloatToBytes(timeSpents[0]*attributesWeight["time_spent"]))

	if len(timeSpents) > 1 {
		dataset.Set(timeSpent2Spec, 0, base.PackFloatToBytes(timeSpents[1]*attributesWeight["time_spent"]))
	}

	if len(timeSpents) > 2 {
		dataset.Set(timeSpent3Spec, 0, base.PackFloatToBytes(timeSpents[2]*attributesWeight["time_spent"]))
	}

	dataset.Set(deviceSpec, 0, base.PackFloatToBytes(encodedDevice))

	return dataset
}

func main() {
	// Parameters for predictions
	categories := []string{"Fashion", "Beauty"}
	articleCounts := []int{10, 9}
	readingRates := []float64{75.0, 70.0}
	timeSpents := []float64{86.0, 75.0}
	device := "Smartphone"

	// Generate random user data
	userData := generateUserData()

	// Create dataset with numeric encoding (only using article_category as feature)
	genderDataset := createDataset(userData, PREDICT_GENDER)

	// Shuffle the dataset using base.Shuffle
	base.Shuffle(genderDataset)

	// Split dataset into training and test sets (70/30 split)
	genderTrainData, genderTestData := base.InstancesTrainTestSplit(genderDataset, 0.7)

	// Create and train a k-NN classifier (or any other golearn classifier)
	genderClassifier := knn.NewKnnClassifier("euclidean", "linear", 3) // k-NN with k=3

	// Train the classifier
	err := genderClassifier.Fit(genderTrainData)
	if err != nil {
		fmt.Println("Error during training gender data:", err)
		return
	}

	// Predict outcomes on the test set
	genderPredictions, err := genderClassifier.Predict(genderTestData)
	if err != nil {
		fmt.Println("Error during gender prediction:", err)
		return
	}

	// Evaluate the model's accuracy
	genderConfusionMat, err := evaluation.GetConfusionMatrix(genderTestData, genderPredictions)
	if err != nil {
		fmt.Println("Error creating confusion matrix for gender test dataset:", err)
		return
	}

	// Print the evaluation summary
	fmt.Println(evaluation.GetSummary(genderConfusionMat))

	genderTestInstance := createSingleInstanceFromTrainData(categories, articleCounts, readingRates, timeSpents, device, PREDICT_GENDER)
	genderSinglePrediction, err := genderClassifier.Predict(genderTestInstance)
	if err != nil {
		fmt.Println("Error during prediction for gender:", err)
		return
	}

	// Convert prediction to float64 and decode the gender
	genderPredictionFloat, err := strconv.ParseFloat(base.GetClass(genderSinglePrediction, 0), 64)
	if err != nil {
		fmt.Println("Error parsing prediction for gender:", err)
		return
	}
	predictedGender := decodeGender(genderPredictionFloat)

	// Create dataset with numeric encoding (using article_category and gender as feature)
	ageGroupDataset := createDataset(userData, PREDICT_AGE_GROUP)

	// Shuffle the dataset using base.Shuffle
	base.Shuffle(ageGroupDataset)

	// Split dataset into training and test sets (70/30 split)
	ageGroupTrainData, ageGroupTestData := base.InstancesTrainTestSplit(ageGroupDataset, 0.7)

	// Create and train a k-NN classifier (or any other golearn classifier)
	ageGroupClassifier := knn.NewKnnClassifier("euclidean", "linear", 3) // k-NN with k=3

	// Train the classifier
	err = ageGroupClassifier.Fit(ageGroupTrainData)
	if err != nil {
		fmt.Println("Error during training age group data:", err)
		return
	}

	// Predict outcomes on the test set
	ageGroupPredictions, err := ageGroupClassifier.Predict(ageGroupTestData)
	if err != nil {
		fmt.Println("Error during age group prediction:", err)
		return
	}

	// Evaluate the model's accuracy
	ageGroupConfusionMat, err := evaluation.GetConfusionMatrix(ageGroupTestData, ageGroupPredictions)
	if err != nil {
		fmt.Println("Error creating confusion matrix for age group test dataset:", err)
		return
	}

	// Print the evaluation summary
	fmt.Println(evaluation.GetSummary(ageGroupConfusionMat))

	ageGroupTestInstance := createSingleInstanceFromTrainData(categories, articleCounts, readingRates, timeSpents, device, PREDICT_AGE_GROUP)
	ageGroupSinglePrediction, err := ageGroupClassifier.Predict(ageGroupTestInstance)
	if err != nil {
		fmt.Println("Error during prediction for age group:", err)
		return
	}

	// Convert prediction to float64 and decode the gender
	ageGroupPredictionFloat, err := strconv.ParseFloat(base.GetClass(ageGroupSinglePrediction, 0), 64)
	if err != nil {
		fmt.Println("Error parsing prediction for age group:", err)
		return
	}
	predictedAgeGroup := decodeAgeGroup(ageGroupPredictionFloat)

	// Create dataset with numeric encoding (using article_category and gender as feature)
	intellectualLevelDataset := createDataset(userData, PREDICT_INTELLECTUAL_LEVEL)

	// Shuffle the dataset using base.Shuffle
	base.Shuffle(intellectualLevelDataset)

	// Split dataset into training and test sets (70/30 split)
	intellectualLevelTrainData, intellectualLevelTestData := base.InstancesTrainTestSplit(intellectualLevelDataset, 0.7)

	// Create and train a k-NN classifier (or any other golearn classifier)
	intellectualLevelClassifier := knn.NewKnnClassifier("euclidean", "linear", 3) // k-NN with k=3

	// Train the classifier
	err = intellectualLevelClassifier.Fit(intellectualLevelTrainData)
	if err != nil {
		fmt.Println("Error during training intellectual level data:", err)
		return
	}

	// Predict outcomes on the test set
	intellectualLevelPredictions, err := intellectualLevelClassifier.Predict(intellectualLevelTestData)
	if err != nil {
		fmt.Println("Error during intellectual level prediction:", err)
		return
	}

	// Evaluate the model's accuracy
	intellectualLevelConfusionMat, err := evaluation.GetConfusionMatrix(intellectualLevelTestData, intellectualLevelPredictions)
	if err != nil {
		fmt.Println("Error creating confusion matrix for intellectual level test dataset:", err)
		return
	}

	// Print the evaluation summary
	fmt.Println(evaluation.GetSummary(intellectualLevelConfusionMat))

	intellectualLevelTestInstance := createSingleInstanceFromTrainData(categories, articleCounts, readingRates, timeSpents, device, PREDICT_INTELLECTUAL_LEVEL)
	intellectualLevelSinglePrediction, err := intellectualLevelClassifier.Predict(intellectualLevelTestInstance)
	if err != nil {
		fmt.Println("Error during prediction for intellectual level:", err)
		return
	}

	// Convert prediction to float64 and decode the gender
	intellectualLevelPredictionFloat, err := strconv.ParseFloat(base.GetClass(intellectualLevelSinglePrediction, 0), 64)
	if err != nil {
		fmt.Println("Error parsing prediction for intellectual level:", err)
		return
	}
	predictedintellectualLevel := decodeIntellectualLevel(intellectualLevelPredictionFloat)

	fmt.Printf("Predicted gender, age group and intellectual level for categories = '%s', articleCounts = %d, readingRates = %.2f, timeSpents = %.2f: %s/%s/%s (accuracy: %.2f/%.2f/%.2f)\n", categories, articleCounts, readingRates, timeSpents, predictedGender, predictedAgeGroup, predictedintellectualLevel, evaluation.GetAccuracy(genderConfusionMat), evaluation.GetAccuracy(ageGroupConfusionMat), evaluation.GetAccuracy(intellectualLevelConfusionMat))
}
