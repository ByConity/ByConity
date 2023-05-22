package org.byconity.hudi;

import org.apache.hadoop.conf.Configuration;
import org.byconity.proto.HudiMeta;
import org.junit.jupiter.api.Test;

import java.net.URL;

public class TestHudiMetastore {

    @Test
    public void fetchHudiTableMeta() throws Exception {
        Configuration conf = new Configuration();
        HudiMetaClient metastore = new HudiMetaClient(conf);
        URL resource = TestHudiMetastore.class.getResource("/test_hudi_mor");
        byte[] ser = metastore.getTable(resource.getPath().toString());
        HudiMeta.HudiTable table = HudiMeta.HudiTable.parseFrom(ser);
        System.out.println(table);
    }
}