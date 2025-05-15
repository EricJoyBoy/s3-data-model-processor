package org.processor;


import lombok.Data;
import org.processor.exception.InputParametersException;

import java.util.Map;
@Data
public class Input {
    private final String owner, executionId, bucket, prefix, table;

    private Input(String owner, String executionId, String bucket, String prefix, String table) {
        this.owner = owner; this.executionId = executionId;
        this.bucket = bucket; this.prefix = prefix;
        this.table = table;
    }

    public static Input from(Map<String, Object> event) {
        String o = (String) event.get("owner");
        String exec = ((String) event.get("sfnExecutionId")).split(":")[7];
        String b = (String) event.get("bucketName");
        String p = (String) event.get("keyPrefix");
        String t = (String) event.get("activityLogsTable");
        if (o == null || exec == null || b == null || p == null || t == null) {
            throw new InputParametersException("Missing parameters");
        }
        return new Input(o, exec, b, p, t);
    }

}