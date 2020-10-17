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

	"github.com/brunokim/prefsp/errors"

	_ "github.com/joho/godotenv/autoload"

	"cloud.google.com/go/firestore"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

var (
	keywords        = flag.String("keywords", "", "Set of comma-separated keywords to track")
	languages       = flag.String("languages", "pt", "Set of comma-separated languages to filter")
	googleProjectID = flag.String("google-project-id", "prefs-2020", "Project ID for Google Firestore database")
)

func main() {
	// Initial setup
	flag.Parse()
	keywords, languages, err := checkFlags()
	if err != nil {
		log.Fatalf("Error while validating flags: %v", err)
	}

	// Connect to Firestore database
	ctx := context.Background()
	db, err := firestore.NewClient(ctx, *googleProjectID)
	if err != nil {
		log.Fatalf("Error while connecting to FireStore DB: %v", err)
	}
	log.Printf("Connected to FireStore DB")

	// Connect to Twitter and start handling messages
	authClient, err := newTwitterAuthClient()
	if err != nil {
		log.Fatalf("Error creating auth client: %v", err)
	}
	client := twitter.NewClient(authClient)
	stream, err := startStream(ctx, db, client, keywords, languages)
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
	db.Close()
}

func checkFlags() ([]string, []string, error) {
	if *keywords == "" {
		return nil, nil, fmt.Errorf("keywords flag cannot be empty")
	}
	terms, err := csv.NewReader(strings.NewReader(*keywords)).Read()
	if err != nil {
		return nil, nil, fmt.Errorf("malformed keywords flag: %v", err)
	}
	if *languages == "" {
		return terms, nil, nil
	}
	langs, err := csv.NewReader(strings.NewReader(*languages)).Read()
	if err != nil {
		return nil, nil, fmt.Errorf("malformed languages flag: %v", err)
	}
	return terms, langs, nil
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

func startStream(ctx context.Context, db *firestore.Client, client *twitter.Client, terms []string, languages []string) (*twitter.Stream, error) {
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
	demux.All = func(msg interface{}) { handleAllMessages(ctx, db, msg) }
	demux.Tweet = func(tweet *twitter.Tweet) { handleTweet(ctx, db, tweet) }
	go demux.HandleChan(stream.Messages)
	return stream, nil
}

func handleAllMessages(ctx context.Context, db *firestore.Client, msg interface{}) {
	if _, ok := msg.(*twitter.Tweet); ok {
		return
	}
	write(ctx, db, "messages", msg)
}

func handleTweet(ctx context.Context, db *firestore.Client, tweet *twitter.Tweet) {
	write(ctx, db, "tweets", tweet)
}

func write(ctx context.Context, db *firestore.Client, collection string, data interface{}) {
	bs, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling struct in %q: %v", collection, err)
		return
	}
	_, result, err := db.Collection(collection).Add(ctx, map[string]interface{}{"data": string(bs)})
	if err != nil {
		log.Printf("Error writing data to %q: %v", collection, err)
	} else {
		log.Printf("Wrote data to %q @ %v", collection, result.UpdateTime)
	}
}
