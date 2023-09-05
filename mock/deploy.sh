#! /bin/bash
# This script assumes that both 'go' and 'aws-cli'
# are properly installed and configured on your system

# Parse flags

i_flag='false'

while getopts 'ih' flag; do
  case ${flag} in
    i) i_flag='true' ;;
    h) printf '%s' \
            '\055i | Optional flag used for creating a new ' \
            'lambda instead of updating an existing one.'
        exit 1 ;;
    *) printf '%s' \
            'Run this script with -h for an explanation of ' \
            'the available commands.'
        exit 1 ;;
  esac
done

# Builds the mock file into a custom bootstrap framework
echo 'Building bootstrap...'
GOOS=linux GOARCH=arm64 go build -v -tags lambda.norpc \
    -o ./deploy/bootstrap main.go

# Zips the bootstrap file
echo 'Zipping bootstrap...'
zip -j ./deploy/containerTest.zip ./deploy/bootstrap

if [ $i_flag == "true" ]
then
    # Creates a new lambda with the zipped bootstrap.
    echo 'Creating...'
    aws lambda create-function --function-name containerTest \
    --runtime provided.al2 --handler bootstrap \
    --architectures arm64 --zip-file fileb://./deploy/containerTest.zip
else
    # Updates the lambda code
    echo 'Uploading.'
    aws lambda update-function-code --function-name containerTest \
    --zip-file fileb://./deploy/containerTest.zip
fi

echo 'Done.'