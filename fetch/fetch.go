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
	follow     = flag.String("follow", "", "Set of comma-separated screen names to track")
	languages  = flag.String("languages", "pt", "Set of comma-separated languages to filter")
	bucketName = flag.String("bucket", "prefs-2020", "Bucket for storing tweets objects")
)

func main() {
	// Initial setup
	flag.Parse()
	keywords, names, languages, bucketName, err := checkFlags()
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

	// Connect to Twitter
	authClient, err := newTwitterAuthClient()
	if err != nil {
		log.Fatalf("Error creating auth client: %v", err)
	}
	client := twitter.NewClient(authClient)

	// Lookup user IDs to follow and start streaming their tweets
	ids, err := lookupUserIds(client, names)
	if err != nil {
		log.Fatalf("Error reading user IDs: %v", err)
	}
	stream, err := startStream(ctx, bucket, client, keywords, ids, languages)
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

func checkFlags() ([]string, []string, []string, string, error) {
	parseCommaFlag := func(name, line string) ([]string, error) {
		if line == "" {
			return nil, nil
		}
		values, err := csv.NewReader(strings.NewReader(line)).Read()
		if err != nil {
			return nil, fmt.Errorf("malformed %s flag: %v", name, err)
		}
		return values, nil
	}
	terms, err1 := parseCommaFlag("keywords", *keywords)
	names, err2 := parseCommaFlag("follow", *follow)
	langs, err3 := parseCommaFlag("languages", *languages)
	if err := errors.NewErrorList(err1, err2, err3); err != nil {
		return nil, nil, nil, "", err
	}
	if len(terms) == 0 && len(names) == 0 {
		return nil, nil, nil, "", fmt.Errorf("keywords and follow flags cannot both be empty")
	}
	if *bucketName == "" {
		return nil, nil, nil, "", fmt.Errorf("empty bucket name flag")
	}
	return terms, names, langs, *bucketName, nil
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

func lookupUserIds(client *twitter.Client, names []string) ([]int64, error) {
	if len(names) == 0 {
		return nil, nil
	}
	users, _, err := client.Users.Lookup(&twitter.UserLookupParams{
		ScreenName:      names,
		IncludeEntities: twitter.Bool(false),
	})
	if err != nil {
		return nil, fmt.Errorf("looking-up users: %v", err)
	}
	m := make(map[string]int64)
	for _, user := range users {
		m[user.ScreenName] = user.ID
	}
	ids := make([]int64, len(names))
	for i, name := range names {
		ids[i] = m[name]
	}
	return ids, nil
}

func startStream(ctx context.Context, bucket *storage.BucketHandle, client *twitter.Client, terms []string, ids []int64, languages []string) (*twitter.Stream, error) {
	follow := make([]string, len(ids))
	for i, id := range ids {
		follow[i] = fmt.Sprintf("%d", id)
	}
	for i, term := range terms {
		terms[i] = strings.TrimSpace(term)
	}
	stream, err := client.Streams.Filter(&twitter.StreamFilterParams{
		Track:         terms,
		Follow:        follow,
		Language:      languages,
		StallWarnings: twitter.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	log.Print("Successfully connected to Twitter stream")
	log.Printf("Terms:\n\t%v", strings.Join(terms, "\n\t"))
	log.Printf("Users:\n\t%v", strings.Join(follow, "\n\t"))
	log.Printf("Languages:\n\t%v", strings.Join(languages, "\n\t"))
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
