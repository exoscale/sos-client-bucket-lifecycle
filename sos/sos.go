package sos

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var CommonConfigOptFns []func(*config.LoadOptions) error

func NewStorageClient(ctx context.Context, zone, accessKey, secretKey string) (*s3.Client, error) {
	opts := []func(*config.LoadOptions) error{}
	if accessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)

	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		sosEndpoint := fmt.Sprintf("https://sos-%s.exo.io", zone)

		o.Region = zone
		o.BaseEndpoint = aws.String(sosEndpoint)
	}), nil
}
