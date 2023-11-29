package cmd_test

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/exoscale/sos-client-bucket-lifecycle/cmd"
	bconfig "github.com/exoscale/sos-client-bucket-lifecycle/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

var client *s3.Client
var bucket = "abucket"
var ctx = context.TODO()

func DeleteBucket() {
	output, _ := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket})
	for _, version := range cmd.SortVersions(cmd.ToVersions(output)) {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &version.Key, VersionId: &version.VersionId})
		if err != nil {
			panic(err)
		}
	}

	prefix := "key1"
	output2, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &bucket, Prefix: &prefix})
	if err != nil {
		panic(err)
	}

	for _, upload := range output2.Uploads {
		_, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &bucket, Key: upload.Key, UploadId: upload.UploadId})
		if err != nil {
			panic(err)
		}
	}

	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket})
	if err != nil {
		panic(err)
	}

}

func CreateConfig() (aws.Config, error) {
	return config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "your_access_key",
				SecretAccessKey: "your_secret_key",
			},
		}))
}

func CreateClient(cfg aws.Config) *s3.Client {
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		sosEndpoint := "http://localhost:9000/"

		o.Region = "us-east-1"
		o.BaseEndpoint = aws.String(sosEndpoint)
		o.UsePathStyle = true
	})
}

func WithClient(f func(client *s3.Client)) {
	cfg, err := CreateConfig()

	if err != nil {
		panic(err)
	}
	client = CreateClient(cfg)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket, ObjectLockEnabledForBucket: true})
	if err != nil {
		panic(err)
	}
	defer DeleteBucket()
	f(client)
}

func LoadConfig(configPath string) bconfig.BucketLifecycleConfiguration {
	cfg, err := cmd.LoadConfig(configPath)
	if err != nil {
		panic(err)
	}
	return *cfg
}

func PutObject(client *s3.Client, key string) *s3.PutObjectOutput {
	output, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader("toto")})
	if err != nil {
		panic(err)
	}
	return output
}

func DeleteObject(client *s3.Client, key string) *s3.DeleteObjectOutput {
	output, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		panic(err)
	}
	return output
}

func DeleteObjectVersion(client *s3.Client, key string, versionId string) {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key, VersionId: &versionId})
	if err != nil {
		panic(err)
	}
}

func CreateMultipartUpload(client *s3.Client, key string) {
	_, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	if err != nil {
		panic(err)
	}
}

func ListMulipartUploads(client *s3.Client) []types.MultipartUpload {
	key := "key1"
	output, err := client.ListMultipartUploads(context.TODO(), &s3.ListMultipartUploadsInput{Bucket: &bucket, Prefix: &key})
	if err != nil {
		panic(err)
	}
	return output.Uploads
}

func ListObjectVersions(client *s3.Client) []cmd.Version {
	output, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &bucket})
	if err != nil {
		panic(err)
	}
	return cmd.SortVersions(cmd.ToVersions(output))
}

func TestExpiration0DaysOneKeyOneVersion(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_0_days.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 2, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestExpiration0DaysOneKeyMultipleVersions(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_0_days.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 3, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestExpiration1DaysOneKeyOneVersion(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_1_days.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
	})
}

func TestNewerNoncurrentVersionsOneKeyOneVersion(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_newer_noncurrent_versions_0.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestNewerNoncurrentVersionsOneKeyMultipleVersions(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_newer_noncurrent_versions_0.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestNewerNoncurrentVersionsMultipleKeysMultipleVersions(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key2")
		PutObject(client, "key2")
		PutObject(client, "key2")
		cfg := LoadConfig("../testdata/rule_with_expiration_newer_noncurrent_versions_0.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 2, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func Test1NewerNoncurrentVersionsOneKeyMultipleVersions(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_newer_noncurrent_versions_1.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 2, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func Test2NewerNoncurrentVersionsOneKeyMultipleVersions(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_newer_noncurrent_versions_2.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 3, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestNonCurrentDays0DaysOneKeyOneVersion(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_non_current_days_0_days.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestNonCurrentDays0DaysOneKeyMultipleVersions(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_non_current_days_0_days.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestNonCurrentDays0DaysMultipleKeysMultipleVersions(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key1")
		PutObject(client, "key2")
		PutObject(client, "key2")
		PutObject(client, "key2")
		cfg := LoadConfig("../testdata/rule_with_expiration_non_current_days_0_days.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 2, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestNonCurrentDays1DaysOneKeyOneVersion(t *testing.T) {
	WithClient(func(client *s3.Client) {
		PutObject(client, "key1")
		cfg := LoadConfig("../testdata/rule_with_expiration_non_current_days_1_days.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions := ListObjectVersions(client)
		require.Equal(t, 2, len(versions))
		require.True(t, versions[0].IsLatest)
		require.True(t, versions[0].DeleteMarker)
	})
}

func TestExpiredObjectDeleteMarker0Days(t *testing.T) {
	WithClient(func(client *s3.Client) {
		result := PutObject(client, "key1")
		DeleteObject(client, "key1")
		DeleteObjectVersion(client, "key1", *result.VersionId)
		versions := ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
		cfg := LoadConfig("../testdata/rule_with_expiration_expired_object_delete_marker_true.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions = ListObjectVersions(client)
		require.Equal(t, 0, len(versions))
	})
}

func TestExpiredObjectDeleteMarker1Days(t *testing.T) {
	WithClient(func(client *s3.Client) {
		result := PutObject(client, "key1")
		DeleteObject(client, "key1")
		DeleteObjectVersion(client, "key1", *result.VersionId)
		versions := ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
		cfg := LoadConfig("../testdata/rule_with_expiration_expired_object_delete_marker_false.json")
		_ = cmd.Execute(client, bucket, cfg)
		versions = ListObjectVersions(client)
		require.Equal(t, 1, len(versions))
	})
}

/*
Minio does not support AbortIncompleteMultipartUpload : https://github.com/minio/minio/issues/13246
func TestAbortIncompleteMultipartUpload0Days(t *testing.T) {
	WithClient(func(client *s3.Client) {
		CreateMultipartUpload(client, "key1")
		CreateMultipartUpload(client, "key1")
		uploads := ListMulipartUploads(client)
		require.Equal(t, 2, len(uploads))
		cfg := LoadConfig("../testdata/rule_with_abort_incomplete_multipart_upload_0_days.json")
		cmd.Execute(client, bucket, cfg)
		require.Equal(t, 0, len(uploads))
	})
}

func TestAbortIncompleteMultipartUpload1Days(t *testing.T) {
	WithClient(func(client *s3.Client) {
		CreateMultipartUpload(client, "key1")
		CreateMultipartUpload(client, "key1")
		uploads := ListMulipartUploads(client)
		require.Equal(t, 2, len(uploads))
		cfg := LoadConfig("../testdata/rule_with_abort_incomplete_multipart_upload_1_days.json")
		cmd.Execute(client, bucket, cfg)
		require.Equal(t, 0, len(uploads))
	})
}
*/

func TestSortVersionsByDate(t *testing.T) {
	version1 := cmd.Version{
		IsLatest:     true,
		LastModified: time.Date(2023, time.November, 29, 12, 0, 0, 0, time.UTC),
		Key:          "key1"}
	version2 := cmd.Version{
		IsLatest:     false,
		LastModified: time.Date(2023, time.November, 29, 13, 0, 0, 0, time.UTC),
		Key:          "key1"}
	version3 := cmd.Version{
		IsLatest:     false,
		LastModified: time.Date(2023, time.November, 29, 14, 0, 0, 0, time.UTC),
		Key:          "key1"}
	versions := []cmd.Version{version1, version2, version3}
	for i := 0; i < 10; i++ {
		rand.Shuffle(len(versions), func(i, j int) {
			versions[i], versions[j] = versions[j], versions[i]
		})
		cmd.SortVersions(versions)
		require.Equal(t, 3, len(versions))
		require.Equal(t, version3, versions[0])
		require.Equal(t, version2, versions[1])
		require.Equal(t, version1, versions[2])
	}
}

func TestSortVersionsByDateAndNames(t *testing.T) {
	version1 := cmd.Version{
		IsLatest:     true,
		LastModified: time.Date(2023, time.November, 29, 12, 0, 0, 0, time.UTC),
		Key:          "key1"}
	version2 := cmd.Version{
		IsLatest:     false,
		LastModified: time.Date(2023, time.November, 29, 13, 0, 0, 0, time.UTC),
		Key:          "key1"}
	version3 := cmd.Version{
		IsLatest:     false,
		LastModified: time.Date(2023, time.November, 29, 14, 0, 0, 0, time.UTC),
		Key:          "key1"}
	version4 := cmd.Version{
		IsLatest:     true,
		LastModified: time.Date(2023, time.November, 29, 12, 0, 0, 0, time.UTC),
		Key:          "key2"}
	version5 := cmd.Version{
		IsLatest:     false,
		LastModified: time.Date(2023, time.November, 29, 13, 0, 0, 0, time.UTC),
		Key:          "key2"}
	version6 := cmd.Version{
		IsLatest:     false,
		LastModified: time.Date(2023, time.November, 29, 14, 0, 0, 0, time.UTC),
		Key:          "key2"}
	versions := []cmd.Version{version1, version2, version3, version4, version5, version6}
	for i := 0; i < 100; i++ {
		rand.Shuffle(len(versions), func(i, j int) {
			versions[i], versions[j] = versions[j], versions[i]
		})
		cmd.SortVersions(versions)
		require.Equal(t, 6, len(versions))
		require.Equal(t, version3, versions[0])
		require.Equal(t, version2, versions[1])
		require.Equal(t, version1, versions[2])
		require.Equal(t, version6, versions[3])
		require.Equal(t, version5, versions[4])
		require.Equal(t, version4, versions[5])
	}
}
