#!/usr/bin/env bash
set -euo pipefail

# Exit codes:
# 0 - success
# 1 - failure (stops on failed dependency update)

# Helper: update dependency or plugin
update_dependency() {
  local groupId=$1
  local artifactId=$2
  local newVersion=$3

  if [[ "$groupId" == "org.apache.maven.plugins" ]]; then
    echo "🔧 Updating Maven plugin: $groupId:$artifactId → $newVersion"
    mvn versions:use-plugin-version -DartifactId="$artifactId" -DnewVersion="$newVersion" -DgenerateBackupPoms=false -q
  else
    echo "📦 Updating dependency: $groupId:$artifactId → $newVersion"
    mvn versions:use-dep-version -Dincludes="$groupId:$artifactId" -DdepVersion="$newVersion" -DgenerateBackupPoms=false
  fi
}

# Helper: test build
test_build() {
  echo "🧪 Running mvn clean verify..."
  if mvn clean verify -B -q; then
    echo "✅ Build successful!"
    return 0
  else
    echo "❌ Build failed!"
    return 1
  fi
}

# Main update loop
dependencies=(
  "org.assertj:assertj-core:3.27.6"
  "org.pitest:pitest-junit5-plugin:1.2.3"
  "org.openjdk.jmh:jmh-core:1.37"
  "org.openjdk.jmh:jmh-generator-annprocess:1.37"
  "org.junit:junit-bom:6.0.0"
  "org.jctools:jctools-core:4.0.5"
#  "org.pitest:pitest-parent:1.20.6"
#  "org.pitest:pitest-maven:1.20.6"
#  "org.codehaus.mojo:exec-maven-plugin:3.6.1"
#  "org.apache.maven.plugins:maven-surefire-plugin:3.5.4"
#  "org.apache.maven.plugins:maven-compiler-plugin:3.14.1"
#  "org.apache.maven.plugins:maven-shade-plugin:3.6.1"
#  "org.apache.maven.plugins:maven-clean-plugin:3.5.0"
)

for dep in "${dependencies[@]}"; do
  IFS=':' read -r group artifact version <<< "$dep"

  echo "------------------------------------------------------------"
  echo "➡️  Updating $group:$artifact to version $version"
  echo "------------------------------------------------------------"

  update_dependency "$group" "$artifact" "$version"

  if test_build; then
    echo "🎉 Successfully updated $group:$artifact to $version"
  else
    echo "⚠️  Reverting changes for $group:$artifact"
    git checkout pom.xml
    echo "🚫 Stopping script due to failed build."
    exit 1
  fi
done

echo "✅ All dependencies updated successfully!"
