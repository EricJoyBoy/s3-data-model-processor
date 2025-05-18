package org.processor.service;


import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import software.amazon.awssdk.services.s3.model.S3Object;



import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;


public class S3Service {
    private final S3Client s3Client = S3Client.create();
    private static final Predicate<S3Object> IS_NOT_FOLDER = s3Object -> !s3Object.key().endsWith("/");

    /**
     * Counts the number of objects (excluding "folders") in an S3 bucket under a given prefix.
     *
     * @param bucketName The name of the S3 bucket.
     * @param keyPrefix  The prefix to filter objects within the bucket.
     * @return The number of objects found.
     */
    public int countObjects(String bucketName, String keyPrefix) {
        return countNonFolderObjects(bucketName, keyPrefix);
    }




    private int countNonFolderObjects(String bucketName, String keyPrefix) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)//
                .prefix(keyPrefix)//
                .build();

        return (int) s3Client.listObjectsV2Paginator(request)
                .stream()
                .flatMap(page -> page.contents().stream())
                .filter(IS_NOT_FOLDER)
                .count();
    }



    /**
     * Fetches the next batch of object keys (as "bucket/key") from S3, skipping a specified number
     * of non-directory objects and reporting each collected object via the consumer.
     *
     * @param bucketName     The S3 bucket name.
     * @param keyPrefix      The key prefix to list.
     * @param batchSize      The maximum number of keys to return.
     * @param offset         The number of non-directory objects to skip before collecting.
     * @param reportConsumer Called with each full path as it's added.
     * @return A list of up to batchSize full paths ("bucket/key").
     */
    public List<String> fetchNextBatch(
            String bucketName,
            String keyPrefix,
            int batchSize,
            int offset,
            Consumer<String> reportConsumer
    ) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(keyPrefix)
                .build();

        return s3Client.listObjectsV2Paginator(request)
                .stream()
                .flatMap(page -> page.contents().stream())
                .filter(IS_NOT_FOLDER)
                .skip(offset)
                .limit(batchSize)
                .peek(s3Object -> reportConsumer.accept(bucketName + "/" + s3Object.key()))
                .map(s3Object -> bucketName + "/" + s3Object.key())
                .collect(Collectors.toList());
    }
}