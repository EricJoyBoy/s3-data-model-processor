package org.processor;

import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;


import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Processes data models from S3, tracks progress in DynamoDB, and handles reporting.
 * <p>
 * This class is a Lambda function handler that processes data models stored in S3.  It
 * retrieves a list of data models, tracks which models have been processed using
 * DynamoDB, and returns a list of data models that need to be processed.  It also
 * handles counting the total number of data models.
 * </p>
 */
public class S3DataModelProcessor implements RequestHandler<Map<String, Object>, Object> {

    private static final String TIMEZONE_ROME = "Europe/Rome";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm.SS.ssssss");

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final int chunkSize;

    /**
     * Constructor for S3DataModelProcessor.
     * Initializes the S3 and DynamoDB clients and retrieves the chunk size from the
     * environment variable.
     */
    public S3DataModelProcessor() {
        this.s3Client = S3Client.create();
        this.dynamoDbClient = DynamoDbClient.create();
        this.chunkSize = Integer.parseInt(System.getenv("CHUNK_SIZE"));
    }

    /**
     * Lambda function handler.
     * <p>
     * This method is the entry point for the Lambda function. It parses the input
     * event, validates the parameters, and orchestrates the data model processing.
     * </p>
     *
     * @param event   The input event containing the parameters.
     * @param context The Lambda context.
     * @return A list of data models to process, or a message if all reports are handled.
     */
    @Override
    public Object handleRequest(Map<String, Object> event, Context context) {
        System.out.println("##### START #####");

        // Parse input parameters
        String owner = (String) event.get("owner");
        String sfnExecutionId = ((String) event.get("sfnExecutionId")).split(":")[7];
        String bucketName = (String) event.get("bucketName");
        String prefix = (String) event.get("keyPrefix");
        String activityLogsTable = (String) event.get("actvityLogsTable");

        validateInputParameters(owner, sfnExecutionId, bucketName, prefix, activityLogsTable);

        String partitionKey = composePk(owner, sfnExecutionId);
        int totalDataModels = handleCountEvent(activityLogsTable, partitionKey, bucketName, prefix);
        List<String> dataModels = processDataModels(activityLogsTable, partitionKey, bucketName, prefix, totalDataModels);

        System.out.println("##### END #####");
        return dataModels.isEmpty() ? "Handled all reports" : dataModels;
    }

    // -------------- Input Validation Methods ----------------------

    /**
     * Validates the input parameters.
     * <p>
     * Checks if any of the required input parameters are null.  If any are null, it
     * throws an InputParametersException.
     * </p>
     *
     * @param params The input parameters to validate.
     * @throws InputParametersException if any of the parameters are null.
     */
    private void validateInputParameters(String... params) {
        if (Arrays.stream(params).anyMatch(Objects::isNull)) {
            throw new InputParametersException("Missing required parameters");
        }
    }

    // -------------- Partition Key Composition ----------------------
    /**
     * Composes the partition key for DynamoDB.
     * <p>
     * Creates a partition key string from the owner and Step Function execution ID.
     * </p>
     *
     * @param owner          The owner of the data.
     * @param sfnExecutionId The Step Function execution ID.
     * @return The composed partition key.
     */
    private String composePk(String owner, String sfnExecutionId) {
        return String.format("OWNER#%s#EXECUTION_ID#%s", owner, sfnExecutionId);
    }

    // -------------- Data Model Counting Methods ----------------------

    /**
     * Handles the counting of data models.
     * <p>
     * This method retrieves the count of data models.  It first checks if a "Count"
     * event exists in DynamoDB.  If it does, it retrieves the count from the event.
     * If not, it counts the objects in S3 and stores the count in DynamoDB.
     * </p>
     *
     * @param tableName    The name of the DynamoDB table.
     * @param partitionKey The partition key.
     * @param bucketName   The name of the S3 bucket.
     * @param prefix       The prefix of the S3 objects.
     * @return The total number of data models.
     */
    private int handleCountEvent(String tableName, String partitionKey, String bucketName, String prefix) {
        // Query for Count events
        Map<String, AttributeValue> attrValues = Map.of(
                ":pk", AttributeValue.builder().s(partitionKey).build(),
                ":et", AttributeValue.builder().s("Count").build()
        );

        QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PartitionKey = :pk")
                .filterExpression("EvenType = :et")
                .expressionAttributeValues(attrValues)
                .build());

        if (response.count() == 0) {
            return countS3Objects(bucketName, prefix, tableName, partitionKey);
        } else if (response.count() == 1) {
            return Integer.parseInt(response.items().get(0).get("Message").s());
        }
        throw new RuntimeException("Too many Count events for " + partitionKey);
    }

    /**
     * Counts the objects in S3.
     * <p>
     * This method lists the objects in the specified S3 bucket with the given prefix
     * and counts the number of objects that are not directories.
     * </p>
     *
     * @param bucketName   The name of the S3 bucket.
     * @param prefix       The prefix of the S3 objects.
     * @param tableName    The name of the DynamoDB table.
     * @param partitionKey The partition key.
     * @return The number of data models in S3.
     */
    private int countS3Objects(String bucketName, String prefix, String tableName, String partitionKey) {
        int count = 0;
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();


        ListObjectsV2Iterable pages = s3Client.listObjectsV2Paginator(request);
        for (ListObjectsV2Response page : pages) {
            for (S3Object object : page.contents()) {
                if (!object.key().endsWith("/")) count++;
            }
        }

        storeCountInDynamoDB(tableName, partitionKey, count);
        return count;
    }

    /**
     * Stores the count in DynamoDB.
     * <p>
     * This method stores the total count of data models in DynamoDB as a "Count" event.
     * </p>
     *
     * @param tableName    The name of the DynamoDB table.
     * @param partitionKey The partition key.
     * @param count        The total number of data models.
     */
    private void storeCountInDynamoDB(String tableName, String partitionKey, int count) {
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(Map.of(
                        "PartitionKey", AttributeValue.builder().s(partitionKey).build(),
                        "EvenType", AttributeValue.builder().s("Count").build(),
                        "DateTime", AttributeValue.builder().s(getCurrentDateTime()).build(),
                        "Message", AttributeValue.builder().s(String.valueOf(count)).build()
                ))
                .build());
    }

    // -------------- Data Model Processing Methods ----------------------

    /**
     * Processes the data models.
     * <p>
     * This method retrieves and processes data models from S3, keeping track of
     * processed reports in DynamoDB.
     * </p>
     *
     * @param tableName     The name of the DynamoDB table.
     * @param partitionKey  The partition key.
     * @param bucketName    The name of the S3 bucket.
     * @param prefix        The prefix of the S3 objects.
     * @param totalDataModels The total number of data models to process.
     * @return A list of data model paths to be processed.
     */
    private List<String> processDataModels(String tableName, String partitionKey, String bucketName,
                                           String prefix, int totalDataModels) {
        // Check existing reports
        QueryResponse response = dynamoDbClient.query(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PartitionKey = :pk")
                .filterExpression("EvenType = :et")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(partitionKey).build(),
                        ":et", AttributeValue.builder().s("Report").build()
                ))
                .build());

        int handledReports = response.count();
        if (handledReports >= totalDataModels) return Collections.emptyList();

        return processNextChunk(bucketName, prefix, tableName, partitionKey, handledReports);
    }

    /**
     * Processes the next chunk of data models.
     * <p>
     * Retrieves a chunk of data models from S3, processes them, and stores report
     * information in DynamoDB.
     * </p>
     *
     * @param bucketName     The name of the S3 bucket.
     * @param prefix         The prefix of the S3 objects.
     * @param tableName      The name of the DynamoDB table.
     * @param partitionKey   The partition key.
     * @param handledReports The number of reports already handled.
     * @return A list of data model paths in the current chunk.
     */
    private List<String> processNextChunk(String bucketName, String prefix, String tableName,
                                          String partitionKey, int handledReports) {
        List<String> dataModels = new ArrayList<>();
        int processed = 0;

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();


        ListObjectsV2Iterable pages = s3Client.listObjectsV2Paginator(request);
        for (ListObjectsV2Response page : pages) {
            for (S3Object object : page.contents()) {
                if (dataModels.size() >= chunkSize) break;
                if (object.key().endsWith("/")) continue;

                if (++processed > handledReports) {
                    String fullPath = bucketName + "/" + object.key();
                    dataModels.add(fullPath);
                    storeReportInDynamoDB(tableName, partitionKey, fullPath);
                }
            }
            if (dataModels.size() >= chunkSize) break;
        }
        return dataModels;
    }

    /**
     * Stores a report in DynamoDB.
     * <p>
     * Stores the path of a processed data model in DynamoDB as a "Report" event.
     * </p>
     *
     * @param tableName    The name of the DynamoDB table.
     * @param partitionKey The partition key.
     * @param fullPath     The full path to the data model in S3.
     */
    private void storeReportInDynamoDB(String tableName, String partitionKey, String fullPath) {
        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(Map.of(
                        "PartitionKey", AttributeValue.builder().s(partitionKey).build(),
                        "EvenType", AttributeValue.builder().s("Report").build(),
                        "DateTime", AttributeValue.builder().s(getCurrentDateTime()).build(),
                        "Message", AttributeValue.builder().s(fullPath).build()
                ))
                .build());
    }

    // -------------- Date/Time Utility Method ----------------------

    /**
     * Gets the current date and time.
     * <p>
     * Returns the current date and time formatted as a string, using the specified
     * time zone (Europe/Rome).
     * </p>
     *
     * @return The current date and time string.
     */
    private static String getCurrentDateTime() {
        return ZonedDateTime.now(ZoneId.of(TIMEZONE_ROME))
                .format(DATE_FORMATTER);
    }

    // -------------- Custom Exception ----------------------

    /**
     * Custom exception for missing input parameters.
     */
    static class InputParametersException extends RuntimeException {
        InputParametersException(String message) {
            super(message);
        }
    }
}
