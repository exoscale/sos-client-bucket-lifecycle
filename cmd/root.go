package cmd

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/exoscale/sos-client-bucket-lifecycle/sos"
)

var (
	bucket     string
	accessKey  string
	secretKey  string
	zone       string
	configPath string
)

func CliExecute() {
	flag.Parse()

	client, err := sos.NewStorageClient(context.TODO(), zone, accessKey, secretKey)
	if err != nil {
		log.Fatalf("Cannot create SOS client on zone %s with acccess key %s\n %v", "", accessKey, err)
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Cannot load configuration: %s\n %v", configPath, err)
	}

	location, err := client.GetBucketLocation(context.TODO(), &s3.GetBucketLocationInput{Bucket: &bucket})
	if err != nil {
		log.Fatalf("Cannot get the location of the bucket : %s", err)
	}

	if zone != string(location.LocationConstraint) {
		client, err = sos.NewStorageClient(context.TODO(), string(location.LocationConstraint), accessKey, secretKey)
		if err != nil {
			log.Fatalf("Cannot create SOS client on zone %s with acccess key %s\n %v", string(location.LocationConstraint), accessKey, err)
		}
	}

	log.Printf("Executing bucket lifecycle configuration")
	if err := Execute(client, bucket, *cfg); err != nil {
		log.Fatalf("Error: %v", err)
	}

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

	log.Printf("Done")
}

func init() {
	flag.StringVar(&bucket, "bucket", "", "Bucket name")
	flag.StringVar(&accessKey, "access-key", "", "Access Key")
	flag.StringVar(&secretKey, "secret-key", "", "Secret key")
	flag.StringVar(&zone, "zone", "ch-gva-2", "Bucket zone")
	flag.StringVar(&configPath, "config", "", "Bucket-lifecycle configuration file path (.json)")
}
