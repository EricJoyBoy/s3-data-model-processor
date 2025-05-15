package org.processor;

public class Config {
    public static int getChunkSize() {
        try { return Integer.parseInt(System.getenv("CHUNK_SIZE")); }
        catch (Exception e) { throw new RuntimeException("Invalid CHUNK_SIZE", e); }
    }
}
