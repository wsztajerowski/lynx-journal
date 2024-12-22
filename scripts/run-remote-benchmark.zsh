#!/bin/zsh

# Explicitly set a user-friendly client name
LOGGER_NAME="Journal benchmark runner"
# Include helper scripts
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/logger.sh"
source "$SCRIPT_DIR/git_helpers.sh"
source "$SCRIPT_DIR/aws_helpers.sh"

# Default values
WORKFLOW_BRANCH="main"
WORKFLOW_NAME="benchmark-runner.yml"
AWS_PROFILE="lynx"
S3_BUCKET="baas-lynx-main"
BENCHMARK_PARAMETERS=("--tag project=lynx-journal")
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
}

# Parse named options
PARAMETERS_START=0
for ARG in "$@"; do
    if [[ $PARAMETERS_START -eq 1 ]]; then
        BENCHMARK_PARAMETERS+=("$ARG")
        continue
    fi

    case $ARG in
        -h|--help) display_usage; exit 0 ;;
        -t=*|--benchmark-type=*) BENCHMARK_TYPE="${ARG#*=}" ;;
        -w=*|--workflow-branch=*) WORKFLOW_BRANCH="${ARG#*=}" ;;
        -p=*|--aws-profile=*) AWS_PROFILE="${ARG#*=}" ;;
        -n=*|--benchmark-name=*) BENCHMARK_NAME="${ARG#*=}" ;;
        -sb|--skip-build) SKIP_BUILD=true ;;
        --) PARAMETERS_START=1 ;;
        *) log ERROR "Unknown option: $ARG"; display_usage; exit 1 ;;
    esac
done

# Validate required options
if [[ -z "$BENCHMARK_TYPE" || ! "$BENCHMARK_TYPE" =~ ^(jmh|jmh-with-async)$ ]]; then
    log ERROR "--benchmark-type is required and must be one of: jmh, jmh-with-async"
    display_usage
    exit 1
fi

# Step 1: Get the current Git branch name
log INFO "Getting current Git branch name..."
BRANCH=$(get_current_branch) || exit 1
log INFO "Current Git branch: $BRANCH"
S3_BENCHMARK_PATH="s3://$S3_BUCKET/$BRANCH/jmh-benchmarks.jar"

# Step 2: Build the Maven project and upload the built JAR file to S3 (if not skipped)
if [[ "$SKIP_BUILD" = false ]]; then
    log INFO "Building Maven project..."
    mvn -q -f "$SCRIPT_DIR/../pom.xml" clean package || exit 1

    # Get the built JAR file (hardcoded path)
    JAR_FILE="$SCRIPT_DIR/../jmh-benchmarks/target/jmh-benchmarks.jar"
    if [[ ! -f "$JAR_FILE" ]]; then
        log ERROR "No JAR file found at $JAR_FILE."
        exit 1
    fi
    log INFO "Built JAR file: $JAR_FILE"
    log INFO "Uploading benchmark file to S3 bucket..."
    upload_to_s3 "$JAR_FILE" "$S3_BENCHMARK_PATH" "$AWS_PROFILE" || exit 1
fi

REQUEST_NO=$(date -u +"%Y%m%d_%H%M%S")
REQUEST_ID="$BENCHMARK_TYPE-$REQUEST_NO"
RESULT_PATH="$BRANCH/$BENCHMARK_TYPE/$REQUEST_NO"

# Step 3: Trigger GitHub Actions workflow with GitHub CLI
log INFO "Triggering GitHub Actions workflow..."
gh workflow run "$WORKFLOW_NAME" \
    --repo "$BENCHMARK_REPO" \
    --ref "$WORKFLOW_BRANCH" \
    -f request_id="$REQUEST_ID" \
    -f results_path="$RESULT_PATH" \
    -f benchmark_type="$BENCHMARK_TYPE" \
    -f benchmark_path="$S3_BENCHMARK_PATH" \
    -f s3_result_bucket="$S3_BUCKET" \
    -f parameters="$BENCHMARK_NAME $BENCHMARK_PARAMETERS --tag branch=$BRANCH --tag type=$BENCHMARK_TYPE"
if [[ $? -ne 0 ]]; then
    log ERROR "Failed to trigger GitHub Actions workflow."
    exit 1
fi

# Step 4: Wait for workflow to finish
sleep 5
RUN_ID=$(gh run list --json databaseId --repo "$BENCHMARK_REPO" --workflow=benchmark-runner.yml --limit 1 -q  '.[0].databaseId')
log INFO "Workflow run successfully with RUN_ID: $RUN_ID"
$SCRIPT_DIR/wait-for-gha-run.sh -id $RUN_ID

# Step 5: Check benchmarks results
QUERY="db.jmh_benchmarks.find(
  {'_id.requestId': '$REQUEST_ID'},
  {'_id': 0,
   'jmhResult.benchmark': 1,
   'jmhResult.primaryMetric.score': 1
  }
);"

# Check if BENCHMARK_DB_CONNECTION_STRING environment variable is defined
if [ -z "$BENCHMARK_DB_CONNECTION_STRING" ]; then
  # If the environment variable is not defined, print the query to the console
  log INFO "Environment variable BENCHMARK_DB_CONNECTION_STRING is not defined."
  log INFO "To see results set BENCHMARK_DB_CONNECTION_STRING to BaaS MongoDB's connection string"
  log INFO "Generated MongoDB Query:"
  echo "$QUERY"
else
  # If the environment variable is defined, execute the query using mongosh
  log INFO "Querying benchmark results..."
  mongosh "$BENCHMARK_DB_CONNECTION_STRING" --eval "$QUERY"
fi

echo "Script completed successfully."
echo "Benchmark outputs: https://eu-central-1.console.aws.amazon.com/s3/buckets/$S3_BUCKET?prefix=$RESULT_PATH/"
