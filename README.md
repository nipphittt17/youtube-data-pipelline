# YouTube Data Engineering Pipeline

A local data engineering project that extracts YouTube channel, playlist, and video metadata through the YouTube Data API v3, then stores raw JSON files in Amazon S3. Apache Airflow orchestrates the ingestion tasks using Docker Compose.

> **Security:** Never commit AWS access keys, secret keys, YouTube API keys, `.env` files, or other credentials to Git. Use a dedicated IAM identity with least-privilege permissions. The static-key setup below is intended for local development only.

## Architecture

```text
YouTube Data API v3
        |
        v
Apache Airflow DAG (Docker Compose)
        |
        +--> Task 1: Channel metadata
        |       └── s3://<bucket>/youtube-<channel>/
        |
        +--> Task 2: Playlist items
        |       └── s3://<bucket>/youtube-videos-<channel>/
        |
        └--> Task 3: Video details
                └── s3://<bucket>/youtube-video-details-<channel>/
```

The DAG currently uses the ID `vdo_raw_to_s3` and runs hourly when its schedule is active. Raw data is written to S3 as JSON; this pipeline does not load data into Snowflake.

## 1. Prerequisites

Install the following on your development machine:

- Docker Desktop, including Docker Compose
- Git
- AWS CLI v2
- A Google/YouTube API key with YouTube Data API v3 enabled
- An AWS account and an S3 bucket in the intended AWS Region

Verify the installations:

```bash
docker --version
docker compose version
git --version
aws --version
```

Start Docker Desktop before running Airflow. The local Docker-based scheduler and tasks run only while the relevant containers are running.

## 2. Set up the repository and Airflow with Docker Compose

From the repository root, make sure the project has a `docker-compose.yaml` file and a DAG file under `dags/`. If you are starting a new Airflow environment, use the official Airflow Docker Compose quick-start and follow its version-specific instructions:

- [Official Airflow Docker Compose quick-start](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

Create the usual Airflow directories if they do not exist:

```bash
mkdir -p dags logs plugins config
```

On macOS, if the Compose file expects `AIRFLOW_UID`, create or update the local `.env` file as described in the Airflow quick-start. A common value is:

```dotenv
AIRFLOW_UID=50000
```

**Important:** The `.env` file used by Docker Compose is separate from a Python virtual environment's activation. Docker Compose reads it for Compose variable substitution and environment configuration.

Initialize the Airflow metadata database once:

```bash
docker compose up airflow-init
```

Start Airflow in the background:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker compose ps
```

Open the Airflow UI at [http://localhost:8080](http://localhost:8080). The official quick-start commonly uses `airflow` / `airflow` for its initial local login; change these credentials if your setup differs and do not expose this development UI publicly.

View logs:

```bash
docker compose logs -f
```

Stop the containers without deleting their persisted volumes:

```bash
docker compose down
```

To restart later:

```bash
docker compose up -d
```

> Avoid `docker compose down --volumes` unless you intentionally want to delete persisted local Airflow database data.

## 3. Configure YouTube API access

1. Open the [Google Cloud Console](https://console.cloud.google.com/).
2. Create or select a project.
3. Enable **YouTube Data API v3**.
4. Create an API key.
5. Restrict the key to the YouTube Data API v3 where appropriate.

The DAG expects these environment variables:

- `API_KEY`
- `DICE_CHANNEL_ID`
- `TADA_CHANNEL_ID`

Use the actual channel IDs, not channel display names or handles, unless your code is updated to resolve handles.

## 4. Create or select an S3 bucket

Choose an S3 bucket and Region. For example, this project has used `ap-southeast-2` (Sydney), but set the Region to match your actual bucket.

Set these values for the DAG:

- `S3_BUCKET`: bucket name only, without `s3://`
- `AWS_DEFAULT_REGION`: the bucket's Region, for example `ap-southeast-2`

The bucket must already exist unless you add bucket-creation logic to the project. The DAG's `s3.put_object()` calls create objects, not buckets.

## 5. Create a least-privilege IAM policy for S3

Create a dedicated IAM user or role for this local project. Do not use root-account credentials. In the AWS Console:

1. Open **IAM → Policies → Create policy**.
2. Choose the JSON editor.
3. Replace `<YOUR_BUCKET_NAME>` with the actual bucket name.
4. Review and create the policy.
5. Attach it to the dedicated IAM user/role used by the local pipeline.

Example policy for this pipeline's read/write/list needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListPipelineBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::<YOUR_BUCKET_NAME>"
    },
    {
      "Sid": "ReadWritePipelineObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::<YOUR_BUCKET_NAME>/youtube-*",
        "arn:aws:s3:::<YOUR_BUCKET_NAME>/youtube-videos-*",
        "arn:aws:s3:::<YOUR_BUCKET_NAME>/youtube-video-details-*"
      ]
    }
  ]
}
```

The prefixes above match this project's current object keys. If you later change the S3 key structure, update the policy resources too. If you enable object versioning, delete objects, use SSE-KMS, or add other AWS services, additional permissions may be required. For SSE-KMS, the IAM principal and KMS key policy may need appropriate KMS permissions.

### Create an access key for local development

If you are using an IAM user with programmatic access, create an access key through **IAM → Users → your dedicated user → Security credentials → Create access key**. Choose the use case that matches your environment, review the warnings, and save the secret securely when it is shown. AWS does not display the secret access key again after you leave the creation screen.

Prefer short-lived credentials or AWS IAM Identity Center where feasible. Do not create root access keys.

## 6. Configure AWS credentials for the local machine

Configure a named AWS CLI profile:

```bash
aws configure --profile youtube-pipeline
```

Enter the values when prompted:

```text
AWS Access Key ID: <YOUR_ACCESS_KEY_ID>
AWS Secret Access Key: <YOUR_SECRET_ACCESS_KEY>
Default region name: ap-southeast-2
Default output format: json
```

These commands save credentials in your local AWS configuration files, usually `~/.aws/credentials` and `~/.aws/config`. They do not automatically make those files available inside a Docker container.

Test that the profile works:

```bash
aws sts get-caller-identity --profile youtube-pipeline
```

Check access to the bucket:

```bash
aws s3 ls s3://<YOUR_BUCKET_NAME>/ --profile youtube-pipeline
```

Test a write and read using a temporary test object:

```bash
printf 'S3 permission test\n' > /tmp/s3-permission-test.txt

aws s3 cp /tmp/s3-permission-test.txt \
  s3://<YOUR_BUCKET_NAME>/youtube-permission-test/s3-permission-test.txt \
  --profile youtube-pipeline

aws s3 cp \
  s3://<YOUR_BUCKET_NAME>/youtube-permission-test/s3-permission-test.txt \
  /tmp/s3-permission-test-downloaded.txt \
  --profile youtube-pipeline

cat /tmp/s3-permission-test-downloaded.txt
```

The example policy above allows these `youtube-*` keys. After testing, remove the test object if desired; deleting it requires `s3:DeleteObject`, which is intentionally not included in the policy. You can leave the test object in place or have an administrator clean it up.

## 7. Pass credentials and project settings into Docker Airflow

The Python code calls `boto3.client("s3", region_name=region)`. Boto3 needs credentials **inside the Airflow task container**, not only on the host.

### Option A — Environment variables (simple local setup)

Add the following variables to the local Compose `.env` file. Keep the file out of Git:

```dotenv
AWS_ACCESS_KEY_ID=<YOUR_ACCESS_KEY_ID>
AWS_SECRET_ACCESS_KEY=<YOUR_SECRET_ACCESS_KEY>
AWS_DEFAULT_REGION=ap-southeast-2

S3_BUCKET=<YOUR_BUCKET_NAME>
API_KEY=<YOUR_YOUTUBE_API_KEY>
DICE_CHANNEL_ID=<YOUR_DICE_CHANNEL_ID>
TADA_CHANNEL_ID=<YOUR_TADA_CHANNEL_ID>
```

Then ensure the Airflow services that execute tasks receive these variables. In the official Airflow Compose file, add the following under the shared `x-airflow-common` → `environment:` section so scheduler/worker/task containers inherit them:

```yaml
AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-ap-southeast-2}
S3_BUCKET: ${S3_BUCKET}
API_KEY: ${API_KEY}
DICE_CHANNEL_ID: ${DICE_CHANNEL_ID}
TADA_CHANNEL_ID: ${TADA_CHANNEL_ID}
```

Check the indentation and merge these entries into the existing `environment:` mapping; do not replace the rest of the Airflow configuration. If you use temporary AWS credentials, also pass `AWS_SESSION_TOKEN` through to the containers.

Recreate the containers so the environment changes take effect:

```bash
docker compose up -d --force-recreate
```

**Note:** environment variables are a convenient local-development method, but secrets may be visible to users who can inspect container configuration. Do not use this pattern for production deployment.

### Option B — Mount the host AWS profile into Airflow

This avoids copying the access key and secret into the Compose `.env` file. First, configure the profile using `aws configure --profile youtube-pipeline` as shown above.

In the shared Airflow Compose service configuration, add the host AWS directory as a read-only mount:

```yaml
volumes:
  - ${HOME}/.aws:/home/airflow/.aws:ro
```

Merge this line into the existing `volumes:` list; do not overwrite existing DAG/log/config mounts. Ensure the mount is present in the containers that execute tasks. Then set the following in the shared Airflow `environment:` section:

```yaml
AWS_PROFILE: youtube-pipeline
AWS_DEFAULT_REGION: ap-southeast-2
S3_BUCKET: ${S3_BUCKET}
API_KEY: ${API_KEY}
DICE_CHANNEL_ID: ${DICE_CHANNEL_ID}
TADA_CHANNEL_ID: ${TADA_CHANNEL_ID}
```

The AWS profile must exist in the mounted `~/.aws` files. The profile name is `youtube-pipeline`, as configured in the earlier command. Restart the Airflow containers:

```bash
docker compose up -d --force-recreate
```

This is still a local-development arrangement. Avoid mounting a broadly privileged personal AWS profile into containers; use a dedicated least-privilege identity.

## 8. Keep credentials out of Git

Add these entries to `.gitignore`:

```gitignore
.env
.env.*
!.env.example

.aws/
credentials
```

You can commit a safe `.env.example` file containing placeholders only:

```dotenv
AWS_DEFAULT_REGION=ap-southeast-2
S3_BUCKET=replace-with-your-bucket-name
API_KEY=replace-with-your-youtube-api-key
DICE_CHANNEL_ID=replace-with-dice-channel-id
TADA_CHANNEL_ID=replace-with-tada-channel-id

# Set these locally only if using Option A.
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
```

Never put real values in `.env.example`. If a secret is accidentally committed, revoke/rotate it immediately; deleting it in a later commit does not remove it from Git history.

## 9. Validate AWS access from inside Airflow

After restarting the containers, check that the scheduler sees the expected configuration without printing any secret values:

```bash
docker compose exec airflow-scheduler bash -lc \
  'echo "$AWS_DEFAULT_REGION"; echo "$S3_BUCKET"; test -n "$API_KEY" && echo "YouTube API key is set"'
```

To verify AWS identity from inside the container, use a short Python check:

```bash
docker compose exec airflow-scheduler python -c \
'import boto3; print(boto3.client("sts").get_caller_identity()["Arn"])'
```

If your Compose service is named differently, replace `airflow-scheduler` with the correct service. This checks the scheduler container; confirm the same credentials are available in whichever service executes your tasks.

For a direct S3 read/write test from the container:

```bash
docker compose exec airflow-scheduler python -c \
'import os, boto3; s3=boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-2")); print(s3.list_objects_v2(Bucket=os.environ["S3_BUCKET"], MaxKeys=5).get("KeyCount", 0))'
```

This requires `s3:ListBucket`. Your actual DAG also requires `s3:PutObject` for uploads and `s3:GetObject` to read playlist JSON in Task 3.

## 10. Run and monitor the DAG

1. Open [Airflow UI](http://localhost:8080).
2. Find `vdo_raw_to_s3`.
3. Enable/unpause the DAG if it is paused.
4. Trigger a manual run while developing.
5. Open each task's logs and check record counts and uploaded S3 keys.
6. In the AWS S3 console, open the configured bucket and look for these prefixes:

```text
youtube-dice/
youtube-sonray/
youtube-videos-dice/
youtube-videos-sonray/
youtube-video-details-dice/
youtube-video-details-sonray/
```

The exact file names include a timestamp and channel ID. A successful Airflow run means the task code completed without raising an error; verify that the expected object keys and non-empty JSON data were actually written.

### Scheduling note

The DAG's `start_date` must not be in the future for scheduled runs to begin. For example:

```python
start_date=datetime(2026, 10, 1),
schedule="@hourly",
catchup=False,
```

A manual trigger can run even when the configured scheduled start date is in the future, so check the scheduler and run type when troubleshooting.

## 11. Common troubleshooting

| Symptom | What to check |
|---|---|
| `Unable to locate credentials` | Credentials are not available inside the task container. Check the Compose environment or mounted `~/.aws` profile. |
| `AccessDenied` on `PutObject` | Check `s3:PutObject`, the bucket/prefix ARN, bucket policy, organization SCPs, and any KMS key policy. |
| `AccessDenied` on `GetObject` | Check `s3:GetObject` and the exact object-key ARN. |
| `AccessDenied` on `ListBucket` | Check `s3:ListBucket` on the bucket ARN, not an object ARN. |
| `NoSuchBucket` or wrong region | Verify `S3_BUCKET` and `AWS_DEFAULT_REGION`. |
| `KeyError: 'API_KEY'` or another environment variable | Ensure the variable is set in Compose and passed to the task-executing container. |
| DAG does not run on schedule | Check whether the DAG is unpaused, scheduler is running, and `start_date` is not in the future. |
| DAG succeeds but no expected data appears | Inspect task logs, record counts, object keys, and the exact bucket. An empty list can still be valid JSON. |
| YouTube API errors or quota failures | Verify YouTube Data API v3 is enabled, the key is valid/restricted correctly, and quota has not been exceeded. |

## 12. Local development vs. Amazon MWAA

This setup is for local development. Docker Desktop must remain running for local scheduled DAGs to execute.

When moving to Amazon MWAA, do not copy long-lived access keys into the DAG or environment. Use the MWAA execution role with the required S3 permissions, store DAGs and supporting files in the configured S3 locations, and configure the MWAA environment's networking and dependencies appropriately. Revisit the IAM policy and deployment process for the MWAA architecture.

## References

- [Apache Airflow: Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [AWS CLI: Configure credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
- [Boto3: Credentials](https://docs.aws.amazon.com/boto3/latest/guide/credentials.html)
- [Amazon S3: Actions, resources, and condition keys](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html)
- [YouTube Data API v3](https://developers.google.com/youtube/v3)
