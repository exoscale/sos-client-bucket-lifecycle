# Exoscale - SOS Client Bucket Lifecycle

## Description

In the absence of Bucket Lifecycle support on SOS, this tool allows the application of a set of rules defined in a JSON file.

## Limitations

- Only versioning enabled buckets are supported
- Filters (prefixes, object sizes) are not supported
- A rule is always considered Enabled whatever its state

## Usage

Be sure to have the Docker [installed](https://docs.docker.com/get-docker).

Define your bucket lifecycle configuration somewhere on your filesystem.
The following example defines a configuration which :

- Expire the objects after 10 days (replaced by a DeleteMarker)
- Keep only 20 non current versions
- Remove the delete markers without non current versions
- Abort the multipart uploads that are older than 7 days

```
cat /bucket-lifecycle-configuration.json
```

```json 
{
    "Rules": [
        {
            "ID": "RULE001",
            "Status": "Enabled",
            "Expiration": {
                "Days": 10,
                "ExpiredObjectDeleteMarker": true
            },
            "NoncurrentVersionExpiration": {
                "NewerNoncurrentVersions": 20
            },
            "AbortIncompleteMultipartUpload": {
                "DaysAfterinitiation": 7
            }
        }
    ]
}
```

Execute the OCI container with the configuration file as a volume :

```sh
docker run \
  -v /bucket-lifecycle-configuration.json:/bucket-lifecycle-configuration.json \ 
  exoscale/sos-client-bucket-lifecycle  \
  --config /bucket-lifecycle-configuration.json \
  --bucket mybucket \
  --zone ch-gva-2 \
  --access-key REDACTED \
  --secret-key REDACTED
```

