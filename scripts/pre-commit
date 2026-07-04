#!/bin/sh

echo "***************************"
echo "Running git pre-commit hook"
echo "***************************"

./gradlew spotlessCheck
exit_code=$?

if [ $exit_code -ne 0 ]; then
  echo "Formatting check failed, fixing it..."
  ./gradlew spotlessApply
  exit 1
fi
