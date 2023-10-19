package config

import (
	"fmt"
)

type BucketLifecycleConfiguration struct {
	Rules []Rule `json:"Rules" validate:"required,dive"`
}

type Rule struct {
	ID                             string                          `json:"ID" validate:"required,alphanum,max=255"`
	Status                         string                          `json:"Status" validate:"required,oneof=Enabled Disabled"`
	Filter                         *Filter                         `json:"Filter,omitempty"`
	Expiration                     *Expiration                     `json:"Expiration,omitempty"`
	NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `json:"NoncurrentVersionExpiration,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `json:"AbortIncompleteMultipartUpload,omitempty"`
}

type Filter struct {
	Prefix                *string    `json:"Prefix,omitempty" validate:"omitempty,dirpath|filepath"`
	ObjectSizeGreaterThan *int64     `json:"ObjectSizeGreaterThan,omitempty" validate:"omitempty,number"`
	ObjectSizeLessThan    *int64     `json:"ObjectSizeLessThan,omitempty" validate:"omitempty,number"`
	And                   *AndFilter `json:"And,omitempty"`
}

type AndFilter struct {
	Prefix                *string `json:"Prefix,omitempty" validate:"omitempty,dirpath|filepath"`
	ObjectSizeGreaterThan *int64  `json:"ObjectSizeGreaterThan,omitempty" validate:"omitempty,number,ltfield=ObjectSizeLessThan"`
	ObjectSizeLessThan    *int64  `json:"ObjectSizeLessThan,omitempty" validate:"omitempty,number,gtfield=ObjectSizeGreaterThan"`
}

type Expiration struct {
	Days                      *int `json:"Days,omitempty" validate:"omitempty,number,min=0"`
	ExpiredObjectDeleteMarker bool `json:"ExpiredObjectDeleteMarker,omitempty" validate:"omitempty,boolean"`
}

type NoncurrentVersionExpiration struct {
	NoncurrentDays          *int `json:"NoncurrentDays,omitempty" validate:"omitempty,number,min=0"`
	NewerNoncurrentVersions *int `json:"NewerNoncurrentVersions,omitempty" validate:"omitempty,number,min=0"`
}

type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation *int `json:"DaysAfterInitiation,omitempty" validate:"number,min=0"`
}

func (blc *BucketLifecycleConfiguration) Validate() error {
	for _, rule := range blc.Rules {
		if rule.AbortIncompleteMultipartUpload == nil && rule.Expiration == nil && rule.NoncurrentVersionExpiration == nil {
			return fmt.Errorf("at least one action needs to be specified in a rule")
		}

	}
	return nil
}
