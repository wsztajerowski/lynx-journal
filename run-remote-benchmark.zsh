#!/bin/zsh

# Default values
WORKFLOW_BRANCH="main"
WORKFLOW_NAME="benchmark-runner.yml"
AWS_PROFILE="lynx"
S3_BUCKET="baas-lynx-main"
BECHMARK_PARAMETERS=()
BENCHMARK_TYPE=""
BENCHMARK_REPO="wsztajerowski/benchmark-as-a-service"

# Function to display usage and exit
display_usage() {
    echo "Usage: $0 --BENCHMARK_TYPE=<type> [--WORKFLOW_BRANCH=<branch>] [--aws-profile=<profile>] [additional parameters]"
    echo "\nOptions:"
    echo "  --benchmark-type=<type>       Required. One of: jmh, jmh-with-async"
    echo "  --workflow-branch=<branch>    Optional. Default: $WORKFLOW_BRANCH"
    echo "  --aws-profile=<profile>      Optional. AWS CLI profile to use. Default: $AWS_PROFILE"
    echo "  --help                       Display this help message and exit"
    exit 0
}

# Parse named options
for arg in "$@"; do
    case $arg in
        --help)
            display_usage;
            exit 0;
            ;;
        --benchmark-type=*)
            BENCHMARK_TYPE="${arg#*=}"
            ;;
        --workflow-branch=*)
            WORKFLOW_BRANCH="${arg#*=}"
            ;;
        --aws-profile=*)
            AWS_PROFILE="${arg#*=}"
            ;;
        *)
            BECHMARK_PARAMETERS+="$arg"
            ;;
    esac
done

# Validate required options
if [[ -z "$BENCHMARK_TYPE" || ! "$BENCHMARK_TYPE" =~ ^(jmh|jmh-with-async)$ ]]; then
    echo "Error: --BENCHMARK_TYPE is required and must be one of: jmh, jmh-with-async"
    display_usage
fi

# Step 1: Build the Maven project
echo "Building Maven project..."
mvn -q clean package
if [[ $? -ne 0 ]]; then
    echo "Error: Maven build failed."
    exit 1
fi

# Get the built JAR file (hardcoded path)
JAR_FILE="jmh-benchmarks/target/jmh-benchmarks.jar"
if [[ ! -f "$JAR_FILE" ]]; then
    echo "Error: No JAR file found at $JAR_FILE."
    exit 1
fi
echo "Built JAR file: $JAR_FILE"

# Step 4: Get the current Git branch name
echo "Getting current Git branch name..."
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to get current Git branch name."
    exit 1
fi
echo "Current Git branch: $BRANCH"

S3_BENCHMARK_PATH="s3://$S3_BUCKET/$BRANCH/jmh-benchmarks.jar"

# Step 3: Upload the built JAR file to S3
echo "Uploading benchmark file to S3 bucket..."
if [[ -n "$AWS_PROFILE" ]]; then
    aws --profile "$AWS_PROFILE" s3 cp "$JAR_FILE" "$S3_BENCHMARK_PATH"
else
    aws s3 cp "$JAR_FILE" "$S3_BENCHMARK_PATH"
fi
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to upload JAR file to S3 bucket."
    exit 1
fi

# Step 2: Check the number of objects in the S3 bucket
#s3://baas-lynx-main/mcmp-queue-1/jmh/output.txt
S3_PATH="s3://$S3_BUCKET/$BRANCH/$BENCHMARK_TYPE/"
echo "Counting objects in S3 bucket: $S3_PATH"
if [[ -n "$AWS_PROFILE" ]]; then
    BENCHMARK_RESULTS_COUNT=$(aws --profile "$AWS_PROFILE" s3 ls "$S3_PATH" --recursive | wc -l)
else
    BENCHMARK_RESULTS_COUNT=$(aws s3 ls "$S3_PATH" --recursive | wc -l)
fi
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to count objects in S3 bucket."
    exit 1
fi
echo "Previously run benchmarks: $BENCHMARK_RESULTS_COUNT"
REQUEST_NO=$((BENCHMARK_RESULTS_COUNT + 1))

# Step 5: Trigger GitHub Actions workflow with GitHub CLI
echo "Triggering GitHub Actions workflow..."
gh workflow run $WORKFLOW_NAME \
    --repo $BENCHMARK_REPO \
    --ref "$WORKFLOW_BRANCH" \
    -f request_id="$BRANCH-$BENCHMARK_TYPE-$REQUEST_NO" \
    -f result_prefix="$BRANCH/$BENCHMARK_TYPE/$REQUEST_NO" \
    -f benchmark_type="$BENCHMARK_TYPE" \
    -f benchmark_path="$S3_BENCHMARK_PATH" \
    -f s3_result_bucket="$S3_BUCKET" \
    -f parameters="$BECHMARK_PARAMETERS"
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to trigger GitHub Actions workflow."
    exit 1
fi

echo "Script completed successfully."
