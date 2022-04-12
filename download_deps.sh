#!/bin/bash

set -u -e

BUCKET=""
OUTPUT_DIR=""

print_usage() {
  echo "Usage:"
  echo "  build_deps.sh --bucket <Bucket> --output_dir <OUTPUT DIR>"
}

while (( "$#" )); do
  if [[ "$1" == "--bucket" ]]; then
    BUCKET="$2"
  elif [[ "$1" == "--output_dir" ]]; then
    OUTPUT_DIR="$2"
  else
    echo "Error: unknown flag $1."
    print_usage
    exit 1
  fi
  shift 2
done

if [[ -z "${BUCKET}" ]]; then
  echo "Error: --bucket is not specified."
  print_usage
  exit 1
fi
if [[ -z "${OUTPUT_DIR}" ]]; then
  echo "Error: --output_dir is not specified."
  print_usage
  exit 1
fi

gsutil cp gs://${BUCKET}/lib/libwhistler.so "${OUTPUT_DIR}"
