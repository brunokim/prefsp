package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/brunokim/prefsp/errors"

	_ "github.com/joho/godotenv/autoload"

	"cloud.google.com/go/storage"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/google/uuid"
)

var (
	keywords   = flag.String("keywords", "", "Set of comma-separated keywords to track")
	languages  = flag.String("languages", "pt", "Set of comma-separated languages to filter")
	bucketName = flag.String("bucket", "prefs-2020", "Bucket for storing tweets objects")
)

func main() {
	// Initial setup
	flag.Parse()
	keywords, languages, bucketName, err := checkFlags()
	if err != nil {
		log.Fatalf("Error while validating flags: %v", err)
	}

	// Connect to Cloud Storage
	ctx := context.Background()
	fs, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Error while connecting to Cloud Storage: %v", err)
	}
	bucket := fs.Bucket(bucketName)
	log.Printf("Connected to Cloud Storage")

	// Connect to Twitter and start handling messages
	authClient, err := newTwitterAuthClient()
	if err != nil {
		log.Fatalf("Error creating auth client: %v", err)
	}
	client := twitter.NewClient(authClient)
	stream, err := startStream(ctx, bucket, client, keywords, languages)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}

	// Block waiting for interrupt
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	// Exit
	log.Print("Closing connections")
	stream.Stop()
	fs.Close()
}

func checkFlags() ([]string, []string, string, error) {
	if *keywords == "" {
		return nil, nil, "", fmt.Errorf("keywords flag cannot be empty")
	}
	terms, err := csv.NewReader(strings.NewReader(*keywords)).Read()
	if err != nil {
		return nil, nil, "", fmt.Errorf("malformed keywords flag: %v", err)
	}
	if *bucketName == "" {
		return nil, nil, "", fmt.Errorf("empty bucket name flag")
	}
	var langs []string
	if *languages != "" {
		langs, err = csv.NewReader(strings.NewReader(*languages)).Read()
		if err != nil {
			return nil, nil, "", fmt.Errorf("malformed languages flag: %v", err)
		}
	}
	return terms, langs, *bucketName, nil
}

func newTwitterAuthClient() (*http.Client, error) {
	get := func(key string) (string, error) {
		value := os.Getenv(key)
		if value == "" {
			return "", fmt.Errorf("missing environment variable %q", key)
		}
		return value, nil
	}
	consumerKey, err1 := get("TWITTER_CONSUMER_KEY")
	consumerSecret, err2 := get("TWITTER_CONSUMER_SECRET")
	accessToken, err3 := get("TWITTER_ACCESS_TOKEN")
	accessSecret, err4 := get("TWITTER_ACCESS_SECRET")
	if err := errors.NewErrorList(err1, err2, err3, err4); err != nil {
		return nil, err
	}
	config := oauth1.NewConfig(consumerKey, consumerSecret)
	token := oauth1.NewToken(accessToken, accessSecret)
	return config.Client(oauth1.NoContext, token), nil
}

func startStream(ctx context.Context, bucket *storage.BucketHandle, client *twitter.Client, terms []string, languages []string) (*twitter.Stream, error) {
	stream, err := client.Streams.Filter(&twitter.StreamFilterParams{
		Track:         terms,
		Language:      languages,
		StallWarnings: twitter.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	log.Printf("Successfully connected to Twitter stream for terms %v in languages %v", terms, languages)
	demux := twitter.NewSwitchDemux()
	demux.All = func(msg interface{}) { handleAllMessages(ctx, bucket, msg) }
	demux.Tweet = func(tweet *twitter.Tweet) { handleTweet(ctx, bucket, tweet) }
	go demux.HandleChan(stream.Messages)
	return stream, nil
}

func handleAllMessages(ctx context.Context, bucket *storage.BucketHandle, msg interface{}) {
	if _, ok := msg.(*twitter.Tweet); ok {
		return
	}
	log.Printf("%T", msg)
	write(ctx, bucket, "messages", msg)
}

func handleTweet(ctx context.Context, bucket *storage.BucketHandle, tweet *twitter.Tweet) {
	write(ctx, bucket, "tweets", tweet)
}

func write(ctx context.Context, bucket *storage.BucketHandle, folder string, data interface{}) {
	bs, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling struct in %q: %v", folder, err)
		return
	}
	date := time.Now().UTC().Format("2006-01-02")
	name := fmt.Sprintf("%s/dt=%s/%v.json", folder, date, uuid.New())
	w := bucket.Object(name).NewWriter(ctx)
	if _, err := w.Write(bs); err != nil {
		log.Printf("Error writing data to %s: %v", name, err)
	} else {
		log.Printf("Wrote data to %s", name)
	}
	if err := w.Close(); err != nil {
		log.Printf("Error closing object %s: %v", name, err)
	}
}
