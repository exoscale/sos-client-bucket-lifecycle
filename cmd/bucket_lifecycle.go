package cmd

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-playground/validator/v10"

	"github.com/exoscale/sos-client-bucket-lifecycle/config"
)

type Version struct {
	Key          string
	IsLatest     bool
	LastModified time.Time
	VersionId    string
	DeleteMarker bool
}

func ToVersions(output *s3.ListObjectVersionsOutput) []Version {
	versions := make([]Version, 0)
	for _, version := range output.Versions {
		versions = append(versions, Version{
			Key:          *version.Key,
			IsLatest:     version.IsLatest,
			LastModified: *version.LastModified,
			VersionId:    *version.VersionId,
			DeleteMarker: false,
		})
	}

	for _, deleteMarker := range output.DeleteMarkers {
		versions = append(versions, Version{
			Key:          *deleteMarker.Key,
			IsLatest:     deleteMarker.IsLatest,
			LastModified: *deleteMarker.LastModified,
			VersionId:    *deleteMarker.VersionId,
			DeleteMarker: true,
		})
	}
	return versions
}

func SortVersions(versions []Version) []Version {
	sort.SliceStable(versions, func(i, j int) bool {
		if versions[i].Key < versions[j].Key {
			return true
		} else if versions[i].Key > versions[j].Key {
			return false
		}
		return versions[i].LastModified.After(versions[j].LastModified)
	})

	return versions
}

func AgeInDays(now, lastModified time.Time) int {
	return int(now.Sub(lastModified).Hours() / 24)
}

func applyAbortIncompleteMultipartUpload(client *s3.Client, bucket *string, rule config.Rule) error {
	if rule.AbortIncompleteMultipartUpload != nil {
		paginator := s3.NewListMultipartUploadsPaginator(client, &s3.ListMultipartUploadsInput{Bucket: bucket})
		for paginator.HasMorePages() {
			out, err := paginator.NextPage(context.TODO())
			if err != nil {
				return err
			}

			for _, upload := range out.Uploads {
				age := AgeInDays(time.Now(), *upload.Initiated)
				if age >= *rule.AbortIncompleteMultipartUpload.DaysAfterInitiation {
					_, err := client.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{Bucket: bucket, Key: upload.Key, UploadId: upload.UploadId})
					if err != nil {
						log.Printf("[abort multipart upload] cannot abort upload %s", *upload.UploadId)
					}
				}
			}
		}
	}
	return nil
}

func applyExpiration(client *s3.Client, bucket *string, rule config.Rule, version Version, age int) bool {
	if rule.Expiration != nil && rule.Expiration.Days != nil && version.IsLatest && !version.DeleteMarker {
		if age >= *rule.Expiration.Days {
			_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bucket, Key: &version.Key})
			if err != nil {
				log.Printf("[expiration] key: %s, version %s cannot be removed\n", version.Key, version.VersionId)
			} else {
				log.Printf("[expiration] key: %s, version %s removed\n", version.Key, version.VersionId)
			}
			return true
		}
	}
	return false
}

func applyNoncurrentVersionExpiration(client *s3.Client, bucket *string, rule config.Rule, version Version, age int, nbVersions int) {
	if rule.NoncurrentVersionExpiration != nil {
		if rule.NoncurrentVersionExpiration.NoncurrentDays != nil && age >= *rule.NoncurrentVersionExpiration.NoncurrentDays {
			_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bucket, Key: &version.Key, VersionId: &version.VersionId})
			if err != nil {
				log.Printf("[non current days] key: %s, version %s cannot be removed\n", version.Key, version.VersionId)
			} else {
				log.Printf("[non current days] key: %s, version %s removed\n", version.Key, version.VersionId)
			}
		} else if rule.NoncurrentVersionExpiration.NewerNoncurrentVersions != nil && nbVersions > *rule.NoncurrentVersionExpiration.NewerNoncurrentVersions {
			_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bucket, Key: &version.Key, VersionId: &version.VersionId})
			if err != nil {
				log.Printf("[newer non current versions] key: %s, version %s cannot be removed\n", version.Key, version.VersionId)
			} else {
				log.Printf("[newer non current versions] key: %s, version %s removed\n", version.Key, version.VersionId)
			}
		}
	}
}

func applyRule(client *s3.Client, bucket *string, rule config.Rule) error {
	versioning, err := client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: bucket})
	if err != nil {
		return err
	}

	if versioning.Status != types.BucketVersioningStatusEnabled {
		log.Fatalf("%s is not a versioned bucket", *bucket)
	}

	var previousLatest Version
	var currentKey string
	var nbVersions int

	expireObjectDeleteMarker := func(version *Version) {
		if rule.Expiration != nil &&
			rule.Expiration.ExpiredObjectDeleteMarker &&
			previousLatest.DeleteMarker &&
			previousLatest.IsLatest && (version == nil || version.Key != previousLatest.Key) && nbVersions == 0 {
			_, err := client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: bucket, Key: &previousLatest.Key, VersionId: &previousLatest.VersionId})
			if err != nil {
				log.Printf("[expire delete marker] key: %s, version %s cannot be removed\n", previousLatest.Key, previousLatest.VersionId)
			} else {
				log.Printf("[expire delete marker] key: %s, version %s removed\n", previousLatest.Key, previousLatest.VersionId)
			}
		}
	}

	if applyAbortIncompleteMultipartUpload(client, bucket, rule) != nil {
		return err
	}

	paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{Bucket: bucket})
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			return err
		}

		versions := SortVersions(ToVersions(output))

		for _, version := range versions {
			expireObjectDeleteMarker(&version)

			if currentKey != "" && version.Key != currentKey {
				nbVersions = 0
			}
			if version.IsLatest {
				previousLatest = version
			}
			if currentKey != "" && version.Key == currentKey && !version.IsLatest {
				nbVersions++
			}
			currentKey = version.Key

			age := AgeInDays(time.Now(), version.LastModified)
			// Expiration is only applied on the latest version of the key.
			// If applied, creates a additional non-current version
			if applyExpiration(client, bucket, rule, version, age) {
				nbVersions++
			}
			applyNoncurrentVersionExpiration(client, bucket, rule, version, age, nbVersions)
		}
	}
	expireObjectDeleteMarker(nil)

	return nil
}

func LoadConfig(configPath string) (*config.BucketLifecycleConfiguration, error) {
	// Open our jsonFile
	jsonFile, err := os.Open(configPath)
	// if we os.Open returns an error then handle it
	if err != nil {
		return nil, err
	}

	defer jsonFile.Close()

	byteValue, _ := io.ReadAll(jsonFile)

	var blc config.BucketLifecycleConfiguration

	_ = json.Unmarshal(byteValue, &blc)

	validate := validator.New()
	if err := validate.Struct(blc); err != nil {
		return nil, err
	}

	if err := blc.Validate(); err != nil {
		return nil, err
	}

	return &blc, nil

}

func Execute(client *s3.Client, bucket string, blc config.BucketLifecycleConfiguration) error {

	for _, rule := range blc.Rules {
		err := applyRule(client, &bucket, rule)
		if err != nil {
			return err
		}
	}

	return nil
}
