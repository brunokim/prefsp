package main

import (
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

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

var (
	keywords  = flag.String("keywords", "", "Set of comma-separated keywords to track")
	languages = flag.String("languages", "pt", "Set of comma-separated languages to filter")
)

func main() {
	// Initial setup
	flag.Parse()
	terms, err1 := csv.NewReader(strings.NewReader(*keywords)).Read()
	languages, err2 := csv.NewReader(strings.NewReader(*languages)).Read()
	if err := errors.NewErrorList(err1, err2); err != nil {
		log.Fatalf("Malformed flag: %v", err)
	}

	// Connect to Twitter and start handling messages
	authClient, err := newTwitterAuthClient()
	if err != nil {
		log.Fatalf("Error creating auth client: %v", err)
	}
	client := twitter.NewClient(authClient)
	stream, err := startStream(client, terms, languages)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}

	// Block waiting for interrupt
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	// Exit
	log.Print("Closing connection")
	stream.Stop()
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

func startStream(client *twitter.Client, terms []string, languages []string) (*twitter.Stream, error) {
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
	demux.All = handleAllMessages
	demux.Tweet = handleTweet
	go demux.HandleChan(stream.Messages)
	return stream, nil
}

func handleAllMessages(message interface{}) {
	if _, ok := message.(*twitter.Tweet); ok {
		return
	}
	bs, _ := json.MarshalIndent(message, "", "  ")
	log.Print(string(bs))
}

func handleTweet(tweet *twitter.Tweet) {
	log.Print(tweet.Text)
}
