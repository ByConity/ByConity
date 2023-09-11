package org.byconity.hudi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.byconity.proto.HudiMeta;
import org.byconity.readers.HudiFileSliceArrowReaderBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class TestHudiFileSliceStream {
    final static BufferAllocator allocator = new RootAllocator();
    /*
CREATE TABLE `test_hudi_mor` (
  `uuid` STRING,
  `ts` int,
  `a` int,
  `b` string,
  `c` array<int>,
  `d` map<string, int>,
  `e` struct<a:int, b:string>)
  USING hudi
TBLPROPERTIES (
  'primaryKey' = 'uuid',
  'preCombineField' = 'ts',
  'type' = 'mor');

spark-sql> select a,b,c,d,e from test_hudi_mor;
a       b       c       d       e
1       hello   [10,20,30]      {"key1":1,"key2":2}     {"a":10,"b":"world"}
*/
    Map<String, String> createScanTestParams() {
        Map<String, String> params = new HashMap<>();
        URL resource = TestHudiFileSliceStream.class.getResource("/test_hudi_mor");
        String basePath = resource.getPath().toString();
        params.put("fetch_size", "4096");
        params.put("base_path", basePath);
        params.put("data_file_path",
                basePath + "/64798197-be6a-4eca-9898-0c2ed75b9d65-0_0-54-41_20230105142938081.parquet");
        params.put("delta_file_paths",
                basePath + "/.64798197-be6a-4eca-9898-0c2ed75b9d65-0_20230105142938081.log.1_0-95-78");
        params.put("hive_column_names",
                "_hoodie_commit_time,_hoodie_commit_seqno,_hoodie_record_key,_hoodie_partition_path,_hoodie_file_name,uuid,ts,a,b,c,d,e");
        params.put("hive_column_types",
                "string#string#string#string#string#string#int#int#string#array<int>#map<string,int>#struct<a:int,b:string>");
        params.put("instant_time", "20230105143305070");
        params.put("data_file_length", "436081");
        params.put("input_format", "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        params.put("serde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        params.put("required_fields", "a,b");
        return params;
    }

    byte[] serialize(Map<String, String> map) {
        HudiMeta.Properties.Builder builder = HudiMeta.Properties.newBuilder();
        map.forEach((k, v) -> {
            HudiMeta.Properties.KeyValue kv = HudiMeta.Properties.KeyValue.newBuilder().setKey(k).setValue(v).build();
            builder.addProperties(kv);
        });
        return builder.build().toByteArray();
    }

    void runScanOnParams(byte[] params) throws IOException {
        HudiFileSliceArrowReaderBuilder builder = HudiFileSliceArrowReaderBuilder.create(params);
        System.out.println(builder.toString());

        ArrowReader stream = builder.build();
        while (stream.loadNextBatch()) {
            stream.getVectorSchemaRoot().getFieldVectors().forEach(System.out::println);
        }
        stream.close();
    }

    @Test
    public void c1DoScanTestOnPrimitiveType() throws IOException {
        Map<String, String> params = createScanTestParams();
        runScanOnParams(serialize(params));
    }
}
