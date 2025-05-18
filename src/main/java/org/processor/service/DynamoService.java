/**
 * Provides a service for interacting with Amazon DynamoDB to manage counts and reports.
 * This class offers methods to retrieve or store a counter, count the number of reports,
 * and store a new report associated with a specific partition key.
 */
package org.processor.service;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Interacts with Amazon DynamoDB to manage counts and reports for a given table.
 */
public class DynamoService {
    private final DynamoDbClient client;

    /**
     * Initializes a new instance of the {@code DynamoService} with a default {@link DynamoDbClient}.
     */
    public DynamoService() {
        this.client = DynamoDbClient.create();
    }

    /**
     * Initializes a new instance of the {@code DynamoService} with a provided {@link DynamoDbClient}.
     * This constructor is useful for dependency injection or testing.
     *
     * @param client The DynamoDB client to use for interactions.
     */
    public DynamoService(DynamoDbClient client) {
        this.client = client;
    }

    /**
     * Retrieves a count associated with a given partition key. If no count exists,
     * it uses the provided {@code Supplier} to generate a new count, stores it in DynamoDB,
     * and returns the new count.
     *
     * @param table   The name of the DynamoDB table.
     * @param pk      The partition key to query for.
     * @param counter A {@code Supplier} that provides a new integer value if no count is found.
     * @return The existing count if found, or the newly generated and stored count.
     * @throws RuntimeException If multiple "Count" events are found for the given partition key.
     */
    public int getOrStoreCount(String table, String pk, Supplier<Integer> counter) {
        QueryResponse response = executeCountQuery(table, pk);

        switch (response.count()) {
            case 1:
                return Integer.parseInt(response.items().get(0).get("Message").s());
            case 0:
                int newCount = counter.get();
                storeCount(table, pk, newCount);
                return newCount;
            default:
                throw new RuntimeException("Multiple Count events found for PartitionKey: " + pk + " in table: " + table);
        }
    }

    /**
     * Counts the number of "Report" events associated with a given partition key in the specified table.
     *
     * @param table The name of the DynamoDB table.
     * @param pk    The partition key to query for.
     * @return The number of "Report" events found.
     */
    public int countReports(String table, String pk) {
        QueryResponse response = executeReportQuery(table, pk);
        return response.count();
    }

    /**
     * Stores a new "Report" event with the given message for a specific partition key in the specified table.
     *
     * @param table   The name of the DynamoDB table.
     * @param pk      The partition key to associate the report with.
     * @param message The message content of the report.
     */
    public void storeReport(String table, String pk, String message) {
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(table)
                .item(Map.of(
                        "PartitionKey", AttributeValue.builder().s(pk).build(),
                        "EventType", AttributeValue.builder().s("Report").build(),
                        "DateTime", AttributeValue.builder().s(ZonedDateTime.now().toString()).build(),
                        "Message", AttributeValue.builder().s(message).build()
                )).build();
        client.putItem(putItemRequest);
    }

    private QueryResponse executeCountQuery(String table, String pk) {
        return client.query(QueryRequest.builder()
                .tableName(table)
                .keyConditionExpression("PartitionKey = :pk")
                .filterExpression("EventType = :et")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(pk).build(),
                        ":et", AttributeValue.builder().s("Count").build()
                )).build());
    }

    private QueryResponse executeReportQuery(String table, String pk) {
        return client.query(QueryRequest.builder()
                .tableName(table)
                .keyConditionExpression("PartitionKey = :pk")
                .filterExpression("EventType = :et")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(pk).build(),
                        ":et", AttributeValue.builder().s("Report").build()
                )).build());
    }

    private void storeCount(String table, String pk, int count) {
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(table)
                .item(Map.of(
                        "PartitionKey", AttributeValue.builder().s(pk).build(),
                        "EventType", AttributeValue.builder().s("Count").build(),
                        "DateTime", AttributeValue.builder().s(ZonedDateTime.now().toString()).build(),
                        "Message", AttributeValue.builder().s(String.valueOf(count)).build()
                )).build();
        client.putItem(putItemRequest);
    }
}