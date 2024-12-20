#!/bin/zsh

# Default values
WORKFLOW_BRANCH="main"
WORKFLOW_NAME="benchmark-runner.yml"
AWS_PROFILE="lynx"
S3_BUCKET="baas-lynx-main"
BENCHMARK_PARAMETERS=()
BENCHMARK_TYPE=""
BENCHMARK_REPO="wsztajerowski/benchmark-as-a-service"
SKIP_BUILD=false

# Function to display usage and exit
display_usage() {
    echo "Usage: $0 -t=<type> [-w=<branch>] [-p=<profile>] [-n=<name>] -- [additional parameters]"
    echo "\nOptions:"
    echo "  -t, --benchmark-type=<type>       Required. One of: jmh, jmh-with-async"
    echo "  -w, --workflow-branch=<branch>    Optional. Default: 'main'"
    echo "  -p, --aws-profile=<profile>       Optional. AWS CLI profile to use"
    echo "  -n, --benchmark-name=<name>       Optional. Filter by benchmark name"
    echo "  -sb, --skip-build                Optional. Skip Maven build step"
    echo "  -h, --help                       Display this help message and exit"
    echo "  --                               Denotes the end of options and start of parameters"
    exit 0
}

# Parse named options
PARAMETERS_START=0
for ARG in "$@"; do
    if [[ $PARAMETERS_START -eq 1 ]]; then
        BENCHMARK_PARAMETERS+=("$ARG")
        continue
    fi

    case $ARG in
        -h|--help)
            display_usage;
            exit 0;
            ;;
        -t=*|--benchmark-type=*)
            BENCHMARK_TYPE="${ARG#*=}"
            ;;
        -w=*|--workflow-branch=*)
            WORKFLOW_BRANCH="${ARG#*=}"
            ;;
        -p=*|--aws-profile=*)
            AWS_PROFILE="${ARG#*=}"
            ;;
        -n=*|--benchmark-name=*)
            BENCHMARK_NAME="${ARG#*=}"
            ;;
        -sb|--skip-build)
            SKIP_BUILD=true
            ;;
        --)
            PARAMETERS_START=1
            ;;
        *)
            echo "Unknown option: $ARG"
            display_usage
            ;;
    esac
done

# Validate required options
if [[ -z "$BENCHMARK_TYPE" || ! "$BENCHMARK_TYPE" =~ ^(jmh|jmh-with-async)$ ]]; then
    echo "Error: --benchmark-type is required and must be one of: jmh, jmh-with-async"
    display_usage
fi

# Step 1: Build the Maven project (if not skipped)
if [[ "$SKIP_BUILD" = false ]]; then
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
fi

# Step 2: Get the current Git branch name
echo "Getting current Git branch name..."
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to get current Git branch name."
    exit 1
fi
echo "Current Git branch: $BRANCH"

S3_BENCHMARK_PATH="s3://$S3_BUCKET/$BRANCH/jmh-benchmarks.jar"

# Step 3: Upload the built JAR file to S3 (if build was not skipped)
if [[ "$SKIP_BUILD" = false ]]; then
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
fi

# Step 4: Check the number of objects in the S3 bucket
S3_PATH="s3://$S3_BUCKET/$BRANCH/$BENCHMARK_TYPE/"
echo "Counting objects in S3 bucket: $S3_PATH"
if [[ -n "$AWS_PROFILE" ]]; then
    BENCHMARK_RESULTS_COUNT=$(aws --profile "$AWS_PROFILE" s3 ls "$S3_PATH" | wc -l)
else
    BENCHMARK_RESULTS_COUNT=$(aws s3 ls "$S3_PATH" | wc -l)
fi
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to count objects in S3 bucket."
    exit 1
fi
echo "Previously run benchmarks: $BENCHMARK_RESULTS_COUNT"
REQUEST_NO=$((BENCHMARK_RESULTS_COUNT + 1))
REQUEST_ID="$BRANCH-$BENCHMARK_TYPE-$REQUEST_NO"
RESULT_PATH="$BRANCH/$BENCHMARK_TYPE/$REQUEST_NO"
# Step 5: Trigger GitHub Actions workflow with GitHub CLI
echo "Triggering GitHub Actions workflow..."
echo gh workflow run $WORKFLOW_NAME \
    --repo $BENCHMARK_REPO \
    --ref "$WORKFLOW_BRANCH" \
    -f request_id="$REQUEST_ID" \
    -f results_path="$RESULT_PATH" \
    -f benchmark_type="$BENCHMARK_TYPE" \
    -f benchmark_path="$S3_BENCHMARK_PATH" \
    -f s3_result_bucket="$S3_BUCKET" \
    -f parameters="$BENCHMARK_NAME $BENCHMARK_PARAMETERS"
if [[ $? -ne 0 ]]; then
    echo "Error: Failed to trigger GitHub Actions workflow."
    exit 1
fi

# Step 6: List active remote Git branches
echo "Listing active remote Git branches..."
#REMOTE_BRANCHES=$(git branch -r | grep -v "HEAD ->" | sed 's/origin\///g' | tr -d ' ' | tr '\n' '|' | sed 's/|$//')
# Get all remote branches and store them in an array
# The sed command removes "origin/" prefix and any leading/trailing whitespace
BRANCHES=($(git branch -r | grep -v "HEAD ->" | sed 's/origin\///g' | sed 's/^[[:space:]]*//g'))
echo "Active remote branches: $BRANCHES"

# Step 7: Create MongoDB aggregation pipeline
cat << EOF
db.jmh_benchmarks.aggregate([
  {
    \$match: {
      \$or: [
        $(for branch in "${BRANCHES[@]}"; do
          echo "{ \"_id.requestId\": { \$regex: \"^${branch}\" } },"
        done)
      ]
      $(if [ ! -z "$BENCHMARK_NAME" ]; then
          echo ",\"_id.benchmarkName\": { \$regex: \"${BENCHMARK_NAME}\" }"
        fi)
    }
  },
  {
    \$group: {
      _id: "\$jmhResult.benchmark",
      score: { \$first: "\$jmhResult.primaryMetric.score" },
      scoreUnit: { \$first: "\$jmhResult.primaryMetric.scoreUnit" }
    }
  }
])
EOF

echo "Script completed successfully."
echo "Benchmark outputs: https://eu-central-1.console.aws.amazon.com/s3/buckets/$S3_BUCKET?prefix=$RESULT_PATH/"
