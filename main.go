package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
)

func main() {
	pullMessage()

	http.ListenAndServe(os.Getenv("PORT"), nil)
}

func ErrorHandler(err error) {
	if err != nil {
		log.Fatalf("error: %s", err)
	}
}

func pullMessage() error {

	err := godotenv.Load("env.yaml")
	ErrorHandler(err)

	ctx := context.Background()
	credentialPath, err := filepath.Abs(os.Getenv("GCLOUDPATH"))

	ErrorHandler(err)

	client, err := pubsub.NewClient(ctx, os.Getenv("PROJECTID"), option.WithCredentialsFile(credentialPath))
	ErrorHandler(err)

	defer client.Close()

	var mu sync.Mutex

	sub := client.Subscription(os.Getenv("SUBSCRIBER"))
	cctx, _ := context.WithCancel(ctx)

	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Println("got message:", string(msg.Data))
		msg.Ack()
	})

	if err != nil {
		return fmt.Errorf("receive: %v", err)
	}
	return nil
}
