package org.byconity.common;

public interface MetaClient {
    /// public static MetaClient(byte[]) { return new(); }

    public byte[] getTable(String table) throws Exception;

    public byte[] getFilesInPartition(String table, String partitionKey) throws Exception;
}