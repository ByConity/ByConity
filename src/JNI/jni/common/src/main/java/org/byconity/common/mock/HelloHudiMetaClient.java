package org.byconity.common.mock;

import com.google.protobuf.InvalidProtocolBufferException;
import org.byconity.common.MetaClient;
import org.byconity.proto.HudiMeta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HelloHudiMetaClient implements MetaClient {
    Map<String, String> properties;

    public HelloHudiMetaClient(Map<String, String> properties) {
        this.properties = properties;
    }

    public static HelloHudiMetaClient create(byte[] raw) throws InvalidProtocolBufferException {
        HudiMeta.HudiMetaClientParams params = HudiMeta.HudiMetaClientParams.parseFrom(raw);
        Map<String, String> param = new HashMap<>();
        List<HudiMeta.Properties.KeyValue> properties = params.getProperties().getPropertiesList();
        properties.stream().forEach(x -> {
            param.put(x.getKey(), x.getValue());
        });
        return new HelloHudiMetaClient(param);
    }

    @Override
    public byte[] getTable(String tableName) throws Exception {
        List<HudiMeta.Properties.KeyValue> allProperties = new ArrayList<>();
        properties.forEach((k, v) -> {
            allProperties.add(HudiMeta.Properties.KeyValue.newBuilder().setKey(k).setValue(v).build());
        });
        HudiMeta.Properties propertyProto = HudiMeta.Properties.newBuilder().addAllProperties(allProperties).build();

        HudiMeta.HudiTable table = HudiMeta.HudiTable.newBuilder()
                .setHiveDbName("Hello")
                .setHiveTableName(tableName)
                .setProperties(propertyProto)
                .build();

        return table.toByteArray();
    }

    @Override
    public byte[] getFilesInPartition(String table, String partitionKey) throws Exception {
        return new byte[0];
    }
}
