package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/proto"
	"go_multithreading/appsinstalled"
)

const (
	normalErrRate = 0.01
)

type AppsInstalled struct {
	DevType string
	DevID   string
	Lat     float64
	Lon     float64
	Apps    []uint32
}

type Stats struct {
	Processed int
	Errors    int
	mu        sync.Mutex
}

func dotRename(path string) error {
	dir, file := filepath.Split(path)
	newPath := filepath.Join(dir, "."+file)
	return os.Rename(path, newPath)
}

func serializeAppsInstalled(apps AppsInstalled) ([]byte, error) {
	ua := &appsinstalled.UserApps{
		Lat:  proto.Float64(apps.Lat),
		Lon:  proto.Float64(apps.Lon),
		Apps: apps.Apps,
	}
	return proto.Marshal(ua)
}

func insertAppsInstalled(mc *memcache.Client, apps AppsInstalled, dryRun bool) bool {
	if dryRun {
		log.Printf("Dry run - would insert: %+v\n", apps)
		return true
	}

	data, err := serializeAppsInstalled(apps)
	if err != nil {
		log.Printf("Serialization error: %v", err)
		return false
	}

	item := &memcache.Item{
		Key:   fmt.Sprintf("%s:%s", apps.DevType, apps.DevID),
		Value: data,
	}

	err = mc.Set(item)
	if err != nil {
		log.Printf("Cannot write to memcached: %v\n", err)
		return false
	}
	return true
}

func parseAppsInstalled(line string) (*AppsInstalled, error) {
	parts := strings.Split(line, "\t")
	if len(parts) < 5 {
		return nil, fmt.Errorf("invalid line format")
	}

	appsStr := strings.Split(parts[4], ",")
	var apps []uint32
	for _, app := range appsStr {
		app = strings.TrimSpace(app)
		if app == "" {
			continue
		}
		id, err := strconv.ParseUint(app, 10, 32)
		if err != nil {
			continue
		}
		apps = append(apps, uint32(id))
	}

	lat, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid latitude: %v", err)
	}
	lon, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid longitude: %v", err)
	}

	return &AppsInstalled{
		DevType: parts[0],
		DevID:   parts[1],
		Lat:     lat,
		Lon:     lon,
		Apps:    apps,
	}, nil
}


func processFile(filename string, mcClients map[string]*memcache.Client, dryRun bool, workers int) error {
    log.Printf("Processing file: %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()

	stats := Stats{}
	lines := make(chan string, 10000)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				apps, err := parseAppsInstalled(line)
				if err != nil {
					stats.mu.Lock()
					stats.Errors++
					stats.mu.Unlock()
					continue
				}

				mc, ok := mcClients[apps.DevType]
				if !ok {
					log.Printf("Unknown device type: %s", apps.DevType)
					stats.mu.Lock()
					stats.Errors++
					stats.mu.Unlock()
					continue
				}

				ok = insertAppsInstalled(mc, *apps, dryRun)
				stats.mu.Lock()
				if ok {
					stats.Processed++
				} else {
					stats.Errors++
				}
				stats.mu.Unlock()
			}
		}()
	}

	scanner := bufio.NewScanner(gz)
	var lineCount int
    for scanner.Scan() {
        lineCount++
        lines <- scanner.Text()
    }
    log.Printf("Read %d lines from %s", lineCount, filename)
    close(lines)

	if err := scanner.Err(); err != nil {
		return err
	}

	wg.Wait()

	if stats.Processed == 0 {
		return dotRename(filename)
	}

	errRate := float64(stats.Errors) / float64(stats.Processed)
	if errRate < normalErrRate {
		log.Printf("Acceptable error rate (%.4f). Successful load\n", errRate)
	} else {
		log.Printf("High error rate (%.4f > %.4f). Failed load\n", errRate, normalErrRate)
	}

	return dotRename(filename)
}

func main() {

	dry := flag.Bool("dry", false, "Dry run (don't insert to memcached)")
	pattern := flag.String("pattern", "/data/appsinstalled/*.tsv.gz", "File pattern")
	idfa := flag.String("idfa", "127.0.0.1:33013", "IDFA memcached address")
	gaid := flag.String("gaid", "127.0.0.1:33014", "GAID memcached address")
	adid := flag.String("adid", "127.0.0.1:33015", "ADID memcached address")
	dvid := flag.String("dvid", "127.0.0.1:33016", "DVID memcached address")
	workers := flag.Int("workers", 8, "Number of worker goroutines")
	flag.Parse()

	mcClients := map[string]*memcache.Client{
		"idfa": memcache.New(*idfa),
		"gaid": memcache.New(*gaid),
		"adid": memcache.New(*adid),
		"dvid": memcache.New(*dvid),
	}

	startTime := time.Now()

	files, err := filepath.Glob(*pattern)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		err := processFile(file, mcClients, *dry, *workers)
		if err != nil {
			log.Printf("Error processing file %s: %v", file, err)
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("Execution time: %s\n", elapsed)
}