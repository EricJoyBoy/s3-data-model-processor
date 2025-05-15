package org.processor;


import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.processor.service.DynamoService;
import org.processor.service.S3Service;
import org.processor.util.KeyBuilder;

import java.util.List;
import java.util.Map;

/**
 * Lambda handler that orchestrates the data model processing.
 */
public class S3DataModelProcessor implements RequestHandler<Map<String, Object>, Object> {

    private final S3Service s3Service;
    private final DynamoService dynamoService;
    private final int chunkSize;

    public S3DataModelProcessor() {
        this.s3Service = new S3Service();
        this.dynamoService = new DynamoService();
        this.chunkSize = Config.getChunkSize();
    }


    public S3DataModelProcessor(S3Service s3Service, DynamoService dynamoService, int chunkSize) {
        this.s3Service = s3Service;
        this.dynamoService = dynamoService;
        this.chunkSize = chunkSize;
    }

    @Override
    public Object handleRequest(Map<String, Object> event, Context context) {
        context.getLogger().log("##### START #####");

        Input input = Input.from(event);
        String pk = KeyBuilder.of(input.getOwner(), input.getExecutionId());

        int total = dynamoService.getOrStoreCount(input.getTable(), pk,
                () -> s3Service.countObjects(input.getBucket(), input.getPrefix()));

        List<String> toProcess = s3Service.fetchNextChunk(
                input.getBucket(), input.getPrefix(), chunkSize,
                dynamoService.countReports(input.getTable(), pk),
                fullPath -> dynamoService.storeReport(input.getTable(), pk, fullPath)
        );

        context.getLogger().log("##### END #####");
        return toProcess.isEmpty() ? "All handled" : toProcess;
    }
}
