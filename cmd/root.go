package cmd

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/exoscale/sos-client-bucket-lifecycle/sos"
)

var (
	bucket     string
	zone       string
	accessKey  string
	secretKey  string
	configPath string
)

func CliExecute() error {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	flag.Parse()

	client, err := sos.NewStorageClient(context.TODO(), zone, accessKey, secretKey)
	if err != nil {
		log.Fatalf("Cannot create SOS client on zone %s with acccess key %s\n %v", zone, accessKey, err)
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Cannot load configuration: %s\n %v", configPath, err)
	}

	log.Printf("Executing bucket lifecycle configuration")
	if err := Execute(client, bucket, *cfg); err != nil {
		log.Fatalf("Error: %v", err)
	}
	log.Printf("Done")

	return nil
}

func init() {
	flag.StringVar(&bucket, "bucket", "", "Bucket name")
	flag.StringVar(&accessKey, "access-key", "", "Access Key")
	flag.StringVar(&secretKey, "secret-key", "", "Secret key")
	flag.StringVar(&zone, "zone", "ch-dk-2", "Bucket zone")
	flag.StringVar(&configPath, "config", "", "Bucket-lifecycle configuration file path (.json)")
}
