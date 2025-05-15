# S3 Data Model Processor

AWS Lambda function for processing S3 objects in chunks with DynamoDB tracking.

## Features
- Tracks processing state in DynamoDB
- Processes objects in configurable chunks
- Handles incremental processing
- Timezone-aware logging

## Prerequisites
- Java 11+
- Maven
- AWS SDK configured

## Build & Deploy
```bash
mvn clean package shade:shade
aws lambda create-function --function-name S3DataProcessor \
  --handler com.example.S3DataModelProcessor::handleRequest \
  --runtime java11 \
  --memory-size 512 \
  --timeout 30 \
  --role <your-lambda-role> \
  --zip-file fileb://target/s3-data-processor.jar