package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/brunokim/prefsp/contexts"

	"cloud.google.com/go/storage"
	"github.com/cheggaaa/pb/v3"
	"github.com/dghubble/go-twitter/twitter"
	"google.golang.org/api/iterator"

	_ "github.com/joho/godotenv/autoload"
)

var (
	bucketName   = flag.String("bucket", "prefs-2020", "Bucket for storing tweets objects")
	inputFolder  = flag.String("input-folder", "tweets", "Folder to look for JSON records")
	outputFolder = flag.String("output-folder", "filtered-tweets", "Folder to write tweets after filtering")
	ingestDate   = flag.String("ingest-date", "", "Ingestion date, in the YYYY-MM-DD format")
)

const (
	NumTweetFetchers = 20
	ReadTimeout      = 5 * time.Second
)

func main() {
	// Initial setup
	flag.Parse()
	bucketName, inFolder, outFolder, date, err := checkFlags()
	if err != nil {
		log.Fatalf("Error while validating flags: %v", err)
	}
	ctx, cancel := contexts.WithInterrupt(context.Background())
	defer cancel()

	// Connect to Cloud Storage
	fs, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Error while connecting to Cloud Storage: %v", err)
	}
	bucket := fs.Bucket(bucketName)
	log.Printf("Connected to Cloud Storage")

	if err := runPipeline(ctx, bucket, inFolder, outFolder, date); err != nil {
		log.Printf("Error in pipeline: %v", err)
	}
	log.Printf("Closing connections")
	fs.Close()
}

func checkFlags() (string, string, string, string, error) {
	if *bucketName == "" {
		return "", "", "", "", fmt.Errorf("empty bucket flag")
	}
	if *inputFolder == "" {
		return "", "", "", "", fmt.Errorf("empty input-folder flag")
	}
	if *outputFolder == "" {
		return "", "", "", "", fmt.Errorf("empty output-folder flag")
	}
	if *ingestDate == "" {
		return "", "", "", "", fmt.Errorf("empty ingestion-date flag")
	}
	if _, err := time.Parse("2006-01-02", *ingestDate); err != nil {
		return "", "", "", "", fmt.Errorf("invalid format for ingestion-date flag: %v", err)
	}
	return *bucketName, *inputFolder, *outputFolder, *ingestDate, nil
}

func runPipeline(ctx context.Context, bucket *storage.BucketHandle, inFolder, outFolder, date string) error {
	prefix := fmt.Sprintf("%s/dt=%s/", inFolder, date)
	output := fmt.Sprintf("%s/%s.jsonl", outFolder, date)
	names, namesErr := objectNames(ctx, bucket, prefix)
	results := readTweets(ctx, bucket, names)
	writeErr := writeTweets(ctx, bucket, output, results)
	bar := pb.StartNew(-1)
	defer bar.Finish()
	for {
		select {
		case err, ok := <-writeErr:
			if !ok {
				return nil
			}
			if err != nil {
				log.Printf("Write error: %v", err)
				continue
			}
			bar.Increment()
		case err := <-namesErr:
			if err != nil {
				return fmt.Errorf("reading object names: %v", err)
			}
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

func objectNames(ctx context.Context, bucket *storage.BucketHandle, folder string) (<-chan string, <-chan error) {
	out := make(chan string)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		query := &storage.Query{Prefix: folder}
		query.SetAttrSelection([]string{"Name", "Size"})
		it := bucket.Objects(ctx, query)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				return
			}
			if err != nil {
				errc <- err
				return
			}
			if attrs.Size == 0 {
				// Folder object
				continue
			}
			select {
			case out <- attrs.Name:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errc
}

type readResult struct {
	name  string
	tweet *twitter.Tweet
	err   error
}

func readTweets(ctx context.Context, bucket *storage.BucketHandle, names <-chan string) <-chan readResult {
	out := make(chan readResult)
	var wg sync.WaitGroup
	wg.Add(NumTweetFetchers)
	for i := 0; i < NumTweetFetchers; i++ {
		go func() {
			tweetReader(ctx, bucket, names, out)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func tweetReader(ctx context.Context, bucket *storage.BucketHandle, names <-chan string, out chan<- readResult) {
	for name := range names {
		tweet, err := readObject(ctx, bucket, name)
		select {
		case out <- readResult{name, tweet, err}:
		case <-ctx.Done():
			return
		}
	}
}

func readObject(ctx context.Context, bucket *storage.BucketHandle, name string) (*twitter.Tweet, error) {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()
	r, err := bucket.Object(name).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting to read %q: %v", name, err)
	}
	data, err := ioutil.ReadAll(r)
	defer r.Close()
	if err != nil {
		return nil, fmt.Errorf("reading %q: %v", name, err)
	}
	var tweet twitter.Tweet
	err = json.Unmarshal(data, &tweet)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling %q: %v", name, err)
	}
	return &tweet, nil
}

func writeTweets(ctx context.Context, bucket *storage.BucketHandle, output string, results <-chan readResult) <-chan error {
	out := make(chan error)
	go func() {
		defer close(out)
		w := bucket.Object(output).NewWriter(ctx)
		defer w.Close()
		for result := range results {
			select {
			case out <- writeTweet(w, result):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func writeTweet(w io.Writer, result readResult) error {
	if result.err != nil {
		return fmt.Errorf("tweet %q: %v", result.name, result.err)
	}
	cleanTweet(result.tweet)
	bs, err := json.Marshal(result.tweet)
	if err != nil {
		return fmt.Errorf("marshaling %q: %v", result.name, err)
	}
	bs = append(bs, '\n')
	_, err = w.Write(bs)
	if err != nil {
		return fmt.Errorf("writing %q: %v", result.name, err)
	}
	return nil
}

func cleanTweet(tweet *twitter.Tweet) {
	// Removing problematic fields for later processing with BigQuery.
	tweet.Entities = nil
	tweet.WithheldInCountries = nil
	if tweet.Place != nil {
		tweet.Place.Attributes = nil
		tweet.Place.BoundingBox = nil
		tweet.Place.Geometry = nil
	}
	tweet.User.ProfileBackgroundColor = ""
	tweet.User.ProfileBackgroundColor = ""
	tweet.User.ProfileLinkColor = ""
	tweet.User.ProfileSidebarBorderColor = ""
	tweet.User.ProfileSidebarFillColor = ""
	tweet.User.ProfileTextColor = ""
	if tweet.QuotedStatus != nil {
		cleanTweet(tweet.QuotedStatus)
	}
	if tweet.RetweetedStatus != nil {
		cleanTweet(tweet.RetweetedStatus)
	}
}
