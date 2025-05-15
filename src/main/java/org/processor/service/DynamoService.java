package org.processor.service;


import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Supplier;

public class DynamoService {
    private final DynamoDbClient client = DynamoDbClient.create();

    public int getOrStoreCount(String table, String pk, Supplier<Integer> counter) {
        QueryResponse resp = client.query(QueryRequest.builder()
                .tableName(table)
                .keyConditionExpression("PartitionKey = :pk")
                .filterExpression("EventType = :et")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(pk).build(),
                        ":et", AttributeValue.builder().s("Count").build()
                )).build());

        if (resp.count() == 1) {
            return Integer.parseInt(resp.items().get(0).get("Message").s());
        } else if (resp.count() == 0) {
            int cnt = counter.get();
            client.putItem(PutItemRequest.builder()
                    .tableName(table)
                    .item(Map.of(
                            "PartitionKey", AttributeValue.builder().s(pk).build(),
                            "EventType", AttributeValue.builder().s("Count").build(),
                            "DateTime", AttributeValue.builder()
                                    .s(ZonedDateTime.now().toString()).build(),
                            "Message", AttributeValue.builder().s(String.valueOf(cnt)).build()
                    )).build());
            return cnt;
        }
        throw new RuntimeException("Multiple Count events for " + pk);
    }

    public int countReports(String table, String pk) {
        QueryResponse resp = client.query(QueryRequest.builder()
                .tableName(table)
                .keyConditionExpression("PartitionKey = :pk")
                .filterExpression("EventType = :et")
                .expressionAttributeValues(Map.of(
                        ":pk", AttributeValue.builder().s(pk).build(),
                        ":et", AttributeValue.builder().s("Report").build()
                )).build());
        return resp.count();
    }

    public void storeReport(String table, String pk, String message) {
        client.putItem(PutItemRequest.builder()
                .tableName(table)
                .item(Map.of(
                        "PartitionKey", AttributeValue.builder().s(pk).build(),
                        "EventType", AttributeValue.builder().s("Report").build(),
                        "DateTime", AttributeValue.builder().s(ZonedDateTime.now().toString()).build(),
                        "Message", AttributeValue.builder().s(message).build()
                )).build());
    }
}