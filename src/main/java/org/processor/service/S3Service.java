package org.processor.service;


import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;


import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;


public class S3Service {
    private final S3Client client = S3Client.create();

    public int countObjects(String bucket, String prefix) {
        int count = 0;
        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucket).prefix(prefix).build();
        for (ListObjectsV2Response page : client.listObjectsV2Paginator(req)) {
            for (S3Object obj : page.contents()) if (!obj.key().endsWith("/")) count++;
        }
        return count;
    }

    public List<String> fetchNextChunk(String bucket, String prefix, int chunkSize,
                                       int skip, Consumer<String> reportConsumer) {
        List<String> models = new ArrayList<>();
        int processed = 0;
        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucket).prefix(prefix).build();

        for (ListObjectsV2Response page : client.listObjectsV2Paginator(req)) {
            for (S3Object obj : page.contents()) {
                if (models.size() >= chunkSize) break;
                if (obj.key().endsWith("/")) continue;

                if (++processed > skip) {
                    String path = bucket + "/" + obj.key();
                    models.add(path);
                    reportConsumer.accept(path);
                }
            }
            if (models.size() >= chunkSize) break;
        }
        return models;
    }
}