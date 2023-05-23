package org.byconity.hudi;

import org.apache.hadoop.conf.Configuration;
import org.byconity.proto.HudiMeta;
import org.junit.jupiter.api.Test;

import java.net.URL;

public class TestHudiMetastore {

    public void testHudiTable(String tablePath) throws Exception {
        Configuration conf = new Configuration();
        HudiMetaClient metastore = new HudiMetaClient(tablePath, conf);
        byte[] ser = metastore.getTable();
        HudiMeta.HudiTable table = HudiMeta.HudiTable.parseFrom(ser);
        System.out.println(table);

        String partitionPath = "";
        if (table.getPartitionColumnNameCount() != 0)
        {
            ser = metastore.getPartitionPaths();
            HudiMeta.PartitionPaths partitions = HudiMeta.PartitionPaths.parseFrom(ser);
            System.out.println(partitions);
            partitionPath = partitions.getPaths(0);
        }
        else
        {
            System.out.println("non partition table");
        }

        metastore.getFilesInPartition(partitionPath);
        HudiMeta.HudiFileSlices fileSlices = HudiMeta.HudiFileSlices.parseFrom(ser);
        System.out.println(fileSlices);
    }

    @Test
    public void testMor() throws Exception {
        URL resource = TestHudiMetastore.class.getResource("/test_hudi_mor");
        testHudiTable(resource.getPath());
    }

    @Test
    public void testPartition() throws Exception {
        URL resource = TestHudiMetastore.class.getResource("/hudi_part_mor_rt");
        testHudiTable(resource.getPath());
    }
}