package main

/*
	Scrapes favicons from domains extracted from the Hackernews sqlite database.
*/

import (
	"flag"
	"os"
	"io/fs"
	"time"
	"net/url"
	"net/http"
	"strings"
	"path/filepath"
	"errors"
	"regexp"
	"fmt"

	"database/sql"
	_ "github.com/mattn/go-sqlite3"

	"github.com/gocolly/colly/v2"

	"github.com/sirupsen/logrus"
)

func init() {
	// Log as Text with color
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	// Log to stdout
	logrus.SetOutput(os.Stdout)
}

// Regular expression for a link token link, used for validation.
const DomainStripRegex = `^((www[0-9]*)).`
var domainStripRegex = regexp.MustCompile(DomainStripRegex)

// Extracts the unsanitized (sub)domain from an URL for claim purposes.
// @param requestURL Original URL string.
// @return (Sub)domain string.
func extractDomainFromURL(requestURL string) string {
	if u, err := url.Parse(strings.ToLower(requestURL)); err == nil {
		return u.Hostname()
	}
	return ""
}

// Extracts the sanitized (sub)domain from an URL for claim purposes.
// @param requestURL Original URL string.
// @return (Sub)domain string.
func extractSanitizedDomainFromURL(requestURL string) string {
	// 1. Remove any common subdomains
	domain := extractDomainFromURL(requestURL)
	domain = domainStripRegex.ReplaceAllString(domain, "")
	return domain
}

func main() {
	var err error

	// Parse input flags
	loglevel := flag.String("log", "", "level of logs to print.")
	inputDatabasePath := flag.String("inputdb", "", "input sqlite database.")
	outputStoragePath := flag.String("outputdir", "store", "output storage directory.")
	limit := flag.Int("limit", -1, "maximum entries to parse")
	minScore := flag.Int("minscore", 10, "minimum required score for items")
	batchRequests := flag.Int("batch", 0, "batch requests")
	parallelRequests := flag.Int("parallel", 100, "parallel requests")
	flag.Parse()

	// Set the log level
	switch *loglevel {
	case "panic", "PANIC":
		logrus.SetLevel(logrus.PanicLevel)
	case "fatal", "FATAL":
		logrus.SetLevel(logrus.FatalLevel)
	case "error", "ERROR":
		logrus.SetLevel(logrus.ErrorLevel)
	case "warn", "WARN":
		logrus.SetLevel(logrus.WarnLevel)
	case "info", "INFO":
		logrus.SetLevel(logrus.InfoLevel)
	case "debug", "DEBUG":
		logrus.SetLevel(logrus.DebugLevel)
	case "trace", "TRACE":
		logrus.SetLevel(logrus.TraceLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}

	// Check arguments
	if inputDatabasePath == nil || *inputDatabasePath == "" {
		logrus.WithField("system", "favicon-collector").Infof("No input sqlite database specified.")
		os.Exit(1)
	}

	// Open input sqlite database
	var dbInput *sql.DB
	if dbInput, err = sql.Open("sqlite3", *inputDatabasePath); err != nil {
		logrus.WithField("system", "favicon-collector").Infof("Failed to open sqlite database: %s", err)
		os.Exit(1)
	}

	// Stats
	statsEntries := 0
	statsDuplicates := 0
	statsRows := 0
	statsScrapesPlanned := 0
	statsScrapesSuccess := 0
	statsScrapesSkipped := 0
	statsScrapesFailed := 0

	// @security
	// Maximum body (file) size, should be reasonable
	const maxBodySize = 512 * 1024 // 512 KiB should be enough for most icons

	// Initialize scraper collector
	collector := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"),
		colly.Async(true),
		colly.AllowURLRevisit(),
		colly.IgnoreRobotsTxt(),
		colly.MaxBodySize(maxBodySize),
	)
	collector.DisableCookies()

	// Parallel requests for each domain
	collector.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: *parallelRequests})

	// Scraper error handler
	collector.OnError(func(r *colly.Response, err error) {
		if r == nil || r.Request == nil {
			logrus.WithField("system", "favicon-collector").Debugf("Null response (%s)", err)
			return
		}
		logrus.WithField("system", "favicon-collector").Debugf("Error during crawling for URL %s: %s", r.Request.URL.String(), err)

		// If Not Found, try alternative paths
		if r.StatusCode == 404 {
			if r.Ctx.GetAny("state") == nil {
				// Retry with alternative path
				r.Request.URL.Path = `/favicon.png`
				r.Ctx.Put("state", 1)
				logrus.WithField("system", "favicon-collector").Debugf("Retrying alternative URL: %s", r.Request.URL.String())
				r.Request.Retry()
				return
			}
		}

		// If this is https, retry once with http scheme
		if r.Request.URL.Scheme == "https" {
			// Retry once with http scheme
			r.Request.URL.Scheme = "http"
			logrus.WithField("system", "favicon-collector").Debugf("Retrying URL: %s", r.Request.URL.String())
			r.Request.Retry()
			return
		} else {
			// Already http scheme, permanent failure

			// Increment stats
			statsScrapesFailed = statsScrapesFailed + 1
		}
	})
	// Scraper download handler
	collector.OnResponse(func(r *colly.Response) {
		if r == nil {
			logrus.WithField("system", "favicon-collector").Debugf("Null response (%s)", err)
			return
		}
		if len(r.Body) == 0 {
			logrus.WithField("system", "favicon-collector").Debugf("Empty response for URL %s", r.Request.URL.String())
			return
		}
		if len(r.Body) == maxBodySize {
			logrus.WithField("system", "favicon-collector").Errorf("Icon likely exceeds maximum size for URL %s", r.Request.URL.String())
		}
		logrus.WithField("system", "favicon-collector").Debugf("Got response for URL %s", r.Request.URL.String())

		// Check for image
		contentType := r.Headers.Get("Content-Type")
		if strings.Index(contentType, "image") > -1 {
			// Save file
			path := r.Ctx.Get("path")
			if path != "" {
				r.Save(path)

				// Increment stats
				statsScrapesSuccess = statsScrapesSuccess + 1
				return
			} else {
				// Invalid path
				logrus.WithField("system", "favicon-collector").Errorf("Invalid context for URL %s", r.Request.URL.String())
				os.Exit(2)
			}
		} else {
			logrus.WithField("system", "favicon-collector").Warningf("Got unexpected Content-Type for URL %s: %s", r.Request.URL.String(), contentType)
		}
	})

	// Iterate over HN items, in order of id (oldest first)
	var limitSql string
	if *limit >= 0 {
		limitSql = fmt.Sprintf("LIMIT %d", *limit)
	}
	if rows, err := dbInput.Query(`
		SELECT url
		FROM items
		WHERE type = 'story' AND url != "" AND by != "" AND deleted != true AND dead != true AND score > $1
		ORDER BY id
		` + limitSql, *minScore); err == sql.ErrNoRows || rows == nil {
		// No results
		logrus.WithField("system", "favicon-collector").Infof("No items found")
	} else if err != nil {
		logrus.WithField("system", "favicon-collector").Errorf("Failed to execute select statement: %s", err)
		os.Exit(2)
	} else {
		// Domain map, stores original URL
		domainMap := map[string]string{}

		// Parse results
		for rows.Next() {
			var url string

			// Get data from database
			if err = rows.Scan(&url); err != nil {
				logrus.WithField("system", "favicon-collector").Errorf("Failed to scan statement: %s", err)
				os.Exit(2)
			}

			// Extract sanitized domain
			domain := extractSanitizedDomainFromURL(url)
			if domain == "" {
				logrus.WithField("system", "favicon-collector").Warningf("Could not extract domain from URL: %s", url)
				continue
			}

			// Add to domain map
			if _, present := domainMap[domain]; present {
				// Already present, just increment stats
				statsDuplicates = statsDuplicates + 1
			} else {
				// Not present yet, add to map
				domainMap[domain] = url

				// Increment stats
				statsEntries = statsEntries + 1
			}

			// Increment stats
			statsRows = statsRows + 1
		}

		logrus.WithField("system", "favicon-collector").Infof("Parsed %d rows", statsRows)

		currentRequests := 0
		totalRequests := 0

		// Iterate over all domains in the map
		for domain, url := range domainMap {
			// Construct output favicon path
			pathOutput := filepath.Join(*outputStoragePath, domain + ".ico")

			// Check if not exists in output storage
			domainExists := false
			{
				if _, err := os.Stat(pathOutput); err == nil {
					domainExists = true
				} else if errors.Is(err, fs.ErrNotExist) {
					domainExists = false
				} else {
					// File error
					logrus.WithField("system", "favicon-collector").Errorf("Failed to stat file '%s': %s", pathOutput, err)
					continue
				}
			}

			totalRequests = totalRequests + 1
			if !domainExists {
				// Extract original domain without sanitation, so we retain hopefully a working original url to crawl
				domainOriginal := extractDomainFromURL(url)

				// Construct URL
				urlFavicon := `https://` + domainOriginal + `/favicon.ico`

				// Perform the crawl for the favicon
				ctx := colly.NewContext()
		    	ctx.Put("path", pathOutput)
				logrus.WithField("system", "favicon-collector").Debugf("Planning scrape: %s", urlFavicon)
				headers := http.Header{}
				headers.Set("Origin", `https//` + domainOriginal + `/`)
				collector.Request("GET", urlFavicon, nil, ctx, headers)
				currentRequests = currentRequests + 1

				// Increment stats
				statsScrapesPlanned = statsScrapesPlanned + 1
			} else {
				// Increment stats
				statsScrapesSkipped = statsScrapesSkipped + 1
			}

			// Check maximum number of requests and wait for batch if necessary
			if *batchRequests > 0 && currentRequests >= *batchRequests {
				logrus.WithField("system", "favicon-collector").Infof("%d requests issued, batch size %d, waiting for batch jobs to finish...", totalRequests, *batchRequests)
				logrus.WithField("system", "favicon-collector").Infof("Valid domains: %d (excluding %d duplicate)", statsEntries, statsDuplicates)
				logrus.WithField("system", "favicon-collector").Infof("Planned scrapes: %d (excluding %d skipped)", statsScrapesPlanned, statsScrapesSkipped)
				logrus.WithField("system", "favicon-collector").Infof("Successful scrapes: %d (excluding %d failed)", statsScrapesSuccess, statsScrapesFailed)
				collector.Wait()
				currentRequests = 0
			}
		}

		// Wait for scrape jobs to finish
		logrus.WithField("system", "favicon-collector").Infof("Valid domains: %d (excluding %d duplicate)", statsEntries, statsDuplicates)
		logrus.WithField("system", "favicon-collector").Infof("Planned scrapes: %d (excluding %d skipped)", statsScrapesPlanned, statsScrapesSkipped)
		logrus.WithField("system", "favicon-collector").Infof("Waiting for scraping jobs to finish...")
		collector.Wait()

		// Print stats
		logrus.WithField("system", "favicon-collector").Infof("Finished scraping")
		logrus.WithField("system", "favicon-collector").Infof("Valid domains: %d (excluding %d duplicate)", statsEntries, statsDuplicates)
		logrus.WithField("system", "favicon-collector").Infof("Planned scrapes: %d (excluding %d skipped)", statsScrapesPlanned, statsScrapesSkipped)
		logrus.WithField("system", "favicon-collector").Infof("Successful scrapes: %d (excluding %d failed)", statsScrapesSuccess, statsScrapesFailed)
	}
}

