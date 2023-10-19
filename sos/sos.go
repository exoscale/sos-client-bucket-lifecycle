package sos

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var CommonConfigOptFns []func(*config.LoadOptions) error

func NewStorageClient(ctx context.Context, zone, profile string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithSharedConfigProfile(profile),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		sosEndpoint := fmt.Sprintf("https://sos-%s.exo.io", zone)

		o.Region = zone
		o.BaseEndpoint = aws.String(sosEndpoint)
	}), nil
}
