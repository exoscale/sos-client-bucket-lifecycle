package cmd

import (
	"context"
	"flag"
	"github.com/exoscale/sos-client-bucket-lifecycle/sos"
	"log"
	"os"
	"os/signal"
)

var (
	bucket     string
	zone       string
	profile    string
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

	client, err := sos.NewStorageClient(context.TODO(), zone, profile)
	if err != nil {
		log.Fatalf("Cannot create SOS client on zone %s with profile %s\n %v", zone, profile, err)
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
	flag.StringVar(&profile, "profile", "", "Profile from your credential file")
	flag.StringVar(&zone, "zone", "ch-dk-2", "Bucket zone")
	flag.StringVar(&configPath, "config", "", "Bucket-lifecycle configuration file path (.json)")
}
