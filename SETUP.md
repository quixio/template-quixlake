# QuixLake Initial Setup Guide

> **Note:** This setup guide is for the experimental QuixLake v2 Timeseries Preview. Configuration options may change before final platform integration.

This guide walks you through the initial configuration of the QuixLake template, including setting up secrets and configuring storage.

## Step 1: Configure Secrets

The template requires several secrets to be configured in your Quix environment. Quix manages secrets automatically during the synchronization process.

For more details on secrets management, see the [Quix Secrets Management documentation](https://quix.io/docs/deploy/secrets-management.html).

### Synchronization Flow

1. **Press the Sync button** in the top right corner of the Quix UI

   <img src="images/sync_button.png" alt="Sync Button Location" width="400">

2. **Quix will prompt you to add secrets** - enter values for any missing secrets

   ![Secrets Dialog](images/secrets.png)

3. **Deploy the pipeline** - Quix will deploy all services with your configured secrets

   ![Sync Dialog](images/sync.png)

### Required Secrets

| Secret Key | Used By | Description |
|------------|---------|-------------|
| `s3_user` | MinIO, API, Catalog, Sink | Username for S3-compatible storage access |
| `s3_secret` | MinIO, API, Catalog, Sink | Password for S3-compatible storage access |
| `postgres_password` | PostgreSQL, Catalog | Password for PostgreSQL database |

### Setting Up Secrets

#### MinIO Credentials (`s3_user` and `s3_secret`)

The initial setup uses **MinIO** as a local S3-compatible storage. Since MinIO is deployed fresh with your environment, you define these credentials yourself:

1. **Choose a username** for `s3_user` (e.g., `admin`, `minio_admin`, etc.)
2. **Choose a strong password** for `s3_secret`

These values will be used to:
- Initialize MinIO with these credentials (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`)
- Authenticate all services that access MinIO (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)

#### PostgreSQL Password (`postgres_password`)

PostgreSQL is used as the metadata backend for the Iceberg Catalog. Since it's deployed fresh:

1. **Choose a strong password** for `postgres_password`

This password will be used to:
- Initialize the PostgreSQL database
- Allow the Catalog service to connect to PostgreSQL

> **Important:** Store these credentials securely. Once set, you cannot retrieve them from Quix - you can only overwrite them with new values. If you lose these credentials, you'll need to reset them and potentially lose access to existing data.

### Example Secret Configuration

```
s3_user:          myadminuser
s3_secret:      MySecureP@ssw0rd!2024
postgres_password: AnotherSecureP@ss!
```

## Step 2: Verify Deployment

After the synchronization completes, verify all services are running:

1. **Check Deployment Status** - Ensure all services start successfully:
   - PostgreSQL
   - MinIO
   - MinIO Proxy
   - Quix TS Datalake Catalog
   - Quix TS Datalake API
   - Quix TS Query UI

2. **Verify MinIO** - Access the MinIO console through the MinIO Proxy public URL to confirm storage is working

## Step 3: Run the Example Pipeline (Optional)

To test your setup with sample data, you first need to switch to the "Example pipeline" group in the pipeline view:

<img src="images/category_picker.png" alt="Group Picker" width="300">

Then:

1. Start the **TSBS Data Generator** job to produce sample time-series data
2. The **TSBS Transformer** and **Quix TS Datalake Sink** services will process and store the data
3. Open the **Query UI** (Data Explorer) to run queries:
   ```sql
   SELECT * FROM sensordata LIMIT 10;
   ```

---

## Migrating to AWS S3

If you want to use AWS S3 instead of the local MinIO storage, you'll need to update several variables.

### Variables to Update

The following variables control S3/storage connectivity and must be updated in **multiple deployments**:

| Variable | Default Value | For AWS S3 |
|----------|---------------|------------|
| `AWS_ENDPOINT_URL` | `http://minio:9000` | `https://s3.<region>.amazonaws.com` |
| `AWS_REGION` | `local` | Your AWS region (e.g., `eu-west-1`, `us-east-1`) |
| `S3_BUCKET` | `quixdatalaketest` | Your AWS S3 bucket name |

### Deployments to Update

You must update these variables in the following deployments:

1. **Quix TS Datalake API**
   - `AWS_ENDPOINT_URL`: Set to `https://s3.<region>.amazonaws.com`
   - `AWS_REGION`: Set to your AWS region
   - `S3_BUCKET`: Set to your bucket name

2. **Quix TS Datalake Catalog**
   - `AWS_REGION`: Set to your AWS region
   - `S3_BUCKET`: Set to your bucket name

3. **quix-ts-datalake-sink**
   - `AWS_ENDPOINT_URL`: Set to `https://s3.<region>.amazonaws.com`
   - `AWS_REGION`: Set to your AWS region
   - `S3_BUCKET`: Set to your bucket name

### Update Secrets for AWS

Update your secrets with AWS IAM credentials:

| Secret Key | Value |
|------------|-------|
| `s3_user` | Your AWS Access Key ID |
| `s3_secret` | Your AWS Secret Access Key |

### Example AWS Configuration

For a bucket named `my-company-datalake` in `eu-west-1`:

**Variables (in each deployment):**
```yaml
AWS_ENDPOINT_URL: https://s3.eu-west-1.amazonaws.com
AWS_REGION: eu-west-1
S3_BUCKET: my-company-datalake
```

**Secrets:**
```
s3_user:     AKIAIOSFODNN7EXAMPLE
s3_secret: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### AWS IAM Requirements

Ensure your AWS IAM user/role has the following permissions on your S3 bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

### Disable or Remove MinIO (Optional)

After migrating to AWS S3, you can optionally stop or remove the MinIO-related deployments to save resources:

- `minio`
- `Minio proxy`

---

## Other S3-Compatible Storage

The same approach works for other S3-compatible storage providers (e.g., Google Cloud Storage, DigitalOcean Spaces, Cloudflare R2):

1. Set `AWS_ENDPOINT_URL` to the provider's S3-compatible endpoint
2. Set `AWS_REGION` as required by the provider
3. Update `S3_BUCKET` to your bucket name
4. Configure `s3_user` and `s3_secret` secrets with your provider's credentials

---

## Troubleshooting

### Services failing to start
- Verify all secrets are configured correctly
- Check that secret names match exactly (`s3_user`, `s3_secret`, `postgres_password`)

### Cannot access MinIO
- Ensure `s3_user` and `s3_secret` secrets are set
- Check MinIO deployment logs for authentication errors

### Catalog connection errors
- Verify `postgres_password` is set correctly
- Check PostgreSQL is running and healthy

### S3 access denied after AWS migration
- Verify AWS credentials are correct
- Check IAM permissions on the S3 bucket
- Ensure `AWS_ENDPOINT_URL` is set to `https://s3.<region>.amazonaws.com` (not MinIO URL)
- Verify `AWS_REGION` matches your bucket's region
