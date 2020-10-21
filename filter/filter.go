package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/brunokim/prefsp/contexts"

	_ "github.com/joho/godotenv/autoload"

	"cloud.google.com/go/storage"
	"github.com/dghubble/go-twitter/twitter"
	"google.golang.org/api/iterator"
)

var (
	bucketName  = flag.String("bucket", "prefs-2020", "Bucket for storing tweets objects")
	inputFolder = flag.String("input-folder", "tweets", "Folder to look for JSON records")
)

const (
	NumTweetFetchers = 20
	ReadTimeout      = 5 * time.Second
)

func main() {
	// Initial setup
	flag.Parse()
	bucketName, folder, err := checkFlags()
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

	if err := runPipeline(ctx, bucket, folder); err != nil {
		log.Printf("Error in pipeline: %v", err)
	}
	log.Printf("Closing connections")
	fs.Close()
}

func checkFlags() (string, string, error) {
	if *bucketName == "" {
		return "", "", fmt.Errorf("empty bucket flag")
	}
	if *inputFolder == "" {
		return "", "", fmt.Errorf("empty input-folder flag")
	}
	return *bucketName, *inputFolder, nil
}

func runPipeline(ctx context.Context, bucket *storage.BucketHandle, folder string) error {
	names, errc := objectNames(ctx, bucket, folder)
	results := readTweets(ctx, bucket, names)
	for {
		select {
		case result, ok := <-results:
			if !ok {
				return nil
			}
			if result.err != nil {
				return result.err
			}
			log.Printf("%q: %s", result.name, result.tweet.Text)
		case err := <-errc:
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
		defer func() { close(out) }()
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

type result struct {
	name  string
	tweet *twitter.Tweet
	err   error
}

func readTweets(ctx context.Context, bucket *storage.BucketHandle, names <-chan string) <-chan result {
	out := make(chan result)
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

func tweetReader(ctx context.Context, bucket *storage.BucketHandle, names <-chan string, out chan<- result) {
	for name := range names {
		tweet, err := readObject(ctx, bucket, name)
		select {
		case out <- result{name, tweet, err}:
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
