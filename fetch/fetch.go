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
	"github.com/brunokim/prefsp/progress"

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
	app, err := appFromFlags()
	if err != nil {
		log.Fatalf("Error while validating flags: %v", err)
	}

	// Connect to Cloud Storage
	ctx := context.Background()
	fs, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Error while connecting to Cloud Storage: %v", err)
	}
	app.bucket = fs.Bucket(app.bucketName)
	log.Printf("Connected to Cloud Storage")

	// Connect to Twitter
	authClient, err := newTwitterAuthClient()
	if err != nil {
		log.Fatalf("Error creating auth client: %v", err)
	}
	client := twitter.NewClient(authClient)

	// Lookup user IDs to follow and start streaming their tweets
	err = app.lookupUserIds(client)
	if err != nil {
		log.Fatalf("Error reading user IDs: %v", err)
	}
	stream, err := app.startStream(ctx, client)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}

	// Block waiting for interrupt
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ch:
	case <-ctx.Done():
	}

	// Exit
	log.Print("Closing connections")
	stream.Stop()
	fs.Close()
}

type app struct {
	terms      []string
	names      []string
	langs      []string
	bucketName string
	bucket     *storage.BucketHandle
	ids        []int64
	logger     *progress.WriterLogger
}

func appFromFlags() (*app, error) {
	parseCommaFlag := func(name, line string) ([]string, error) {
		if line == "" {
			return nil, nil
		}
		values, err := csv.NewReader(strings.NewReader(line)).Read()
		if err != nil {
			return nil, fmt.Errorf("malformed %s flag: %v", name, err)
		}
		for i, value := range values {
			values[i] = strings.TrimSpace(value)
		}
		return values, nil
	}
	terms, err1 := parseCommaFlag("keywords", *keywords)
	names, err2 := parseCommaFlag("follow", *follow)
	langs, err3 := parseCommaFlag("languages", *languages)
	if err := errors.NewErrorList(err1, err2, err3); err != nil {
		return nil, err
	}
	if len(terms) == 0 && len(names) == 0 {
		return nil, fmt.Errorf("keywords and follow flags cannot both be empty")
	}
	if *bucketName == "" {
		return nil, fmt.Errorf("empty bucket name flag")
	}
	return &app{
		terms:      terms,
		names:      names,
		langs:      langs,
		bucketName: *bucketName,
	}, nil
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

func (app *app) lookupUserIds(client *twitter.Client) error {
	if len(app.names) == 0 {
		return nil
	}
	users, _, err := client.Users.Lookup(&twitter.UserLookupParams{
		ScreenName:      app.names,
		IncludeEntities: twitter.Bool(false),
	})
	if err != nil {
		return fmt.Errorf("looking-up users: %v", err)
	}
	log.Print("Successfully read user ids for followed users")
	m := make(map[string]int64)
	for _, user := range users {
		m[user.ScreenName] = user.ID
	}
	app.ids = make([]int64, len(app.names))
	for i, name := range app.names {
		var ok bool
		app.ids[i], ok = m[name]
		if !ok {
			log.Printf("WARNING: no user ID found for %s", name)
		}
	}
	return nil
}

func (app *app) startStream(ctx context.Context, client *twitter.Client) (*twitter.Stream, error) {
	follow := make([]string, len(app.ids))
	for i, id := range app.ids {
		follow[i] = fmt.Sprintf("%d", id)
	}
	stream, err := client.Streams.Filter(&twitter.StreamFilterParams{
		Track:         app.terms,
		Follow:        follow,
		Language:      app.langs,
		StallWarnings: twitter.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	log.Print("Successfully connected to Twitter stream")
	log.Printf("Terms:\n\t%v", strings.Join(app.terms, "\n\t"))
	log.Printf("Users:\n\t%v", strings.Join(follow, "\n\t"))
	log.Printf("Languages:\n\t%v", strings.Join(app.langs, "\n\t"))
	app.logger = progress.NewWriterLogger(time.Minute)
	demux := twitter.NewSwitchDemux()
	demux.All = func(msg interface{}) { app.handleAllMessages(ctx, msg) }
	demux.Tweet = func(tweet *twitter.Tweet) { app.handleTweet(ctx, tweet) }
	go demux.HandleChan(stream.Messages)
	return stream, nil
}

func (app *app) handleAllMessages(ctx context.Context, msg interface{}) {
	if _, ok := msg.(*twitter.Tweet); ok {
		return
	}
	log.Printf("%T", msg)
	app.write(ctx, "messages", msg)
}

func (app *app) handleTweet(ctx context.Context, tweet *twitter.Tweet) {
	app.write(ctx, "tweets", tweet)
}

func (app *app) write(ctx context.Context, folder string, data interface{}) {
	bs, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling struct in %q: %v", folder, err)
		return
	}
	date := time.Now().UTC().Format("2006-01-02")
	name := fmt.Sprintf("%s/dt=%s/%v.json", folder, date, uuid.New())
	w := app.bucket.Object(name).NewWriter(ctx)
	if n, err := w.Write(bs); err != nil {
		log.Printf("Error writing data to %s: %v", name, err)
	} else {
		app.logger.Wrote(n)
	}
	if err := w.Close(); err != nil {
		log.Printf("Error closing object %s: %v", name, err)
	}
}
