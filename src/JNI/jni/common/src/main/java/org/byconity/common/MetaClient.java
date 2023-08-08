package org.byconity.common;

public interface MetaClient {
    /// public static MetaClient(byte[]) { return new(); }

    public byte[] getTable() throws Exception;

    public byte[] getPartitionPaths() throws Exception;

    public byte[] getFilesInPartition(String partitionPath) throws Exception;

}