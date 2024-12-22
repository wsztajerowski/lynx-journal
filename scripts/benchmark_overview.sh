#!/bin/bash

QUERY="db.getCollection('jmh_benchmarks').aggregate([
 {
   \$match: {
     'benchmarkMetadata.tags.project': 'lynx-journal',
     'benchmarkMetadata.tags.type': 'jmh'
   }
 },
 {
   \$sort: { 'jmhResult.primaryMetric.score': -1 }
 },
 {
   \$group: {
     _id: {
       benchmark: '\$jmhResult.benchmark',
       branch: '\$benchmarkMetadata.tags.branch'
     },
     requestId: { \$first: '\$_id.requestId' },
     score: { \$first: '\$jmhResult.primaryMetric.score' },
     scoreUnit: { \$first: '\$jmhResult.primaryMetric.scoreUnit' }
   }
 },
 {
   \$project: {
     _id: 0,
     benchmark: '\$_id.benchmark',
     branch: '\$_id.branch',
     score: { \$round: ['\$score', 0] },
     scoreUnit: 1,
     requestId: 1
   }
 },
 {
   \$sort: { 'benchmark': 1, 'score': -1 }
 }
],{ maxTimeMS: 60000, allowDiskUse: true })
.forEach(function(doc) {
   print(doc.benchmark.split('.').pop() + '\\t' + doc.score.toLocaleString('pl-PL') + '\\t' + doc.scoreUnit + '\\t' + doc.branch + '\\t' + doc.requestId);
});"

# Check if BENCHMARK_DB_CONNECTION_STRING environment variable is defined
if [ -z "$BENCHMARK_DB_CONNECTION_STRING" ]; then
  # If the environment variable is not defined, print the query to the console
  echo "Environment variable BENCHMARK_DB_CONNECTION_STRING is not defined."
  echo "Generated MongoDB Query:"
  echo "$QUERY"
else
  # If the environment variable is defined, execute the query using mongosh
  echo "Executing MongoDB query..."
  mongosh "$BENCHMARK_DB_CONNECTION_STRING" --eval "$QUERY" |
  (
    # Print header
    echo -e "BENCHMARK\\tSCORE\\tUNIT\\tBRANCH\\tREQUEST_ID" && cat
  ) | column -t -s $'\t'
fi
