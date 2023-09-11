package org.byconity.hudi;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.byconity.common.MetaClient;
import org.byconity.proto.HudiMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class HudiMetaClient implements MetaClient {
    private static final Logger LOG = LogManager.getLogger(HudiMetaClient.class);

    private Configuration configuration;
    private String basePath;

    public static HudiMetaClient create(byte[] raw) throws InvalidProtocolBufferException {
        HudiMeta.HudiMetaClientParams pb_params = HudiMeta.HudiMetaClientParams.parseFrom(raw);
        /// TODO: build config from map
        Map<String, String> params = pb_params.getProperties().getPropertiesList().stream().collect(
                Collectors.toMap(HudiMeta.Properties.KeyValue::getKey, HudiMeta.Properties.KeyValue::getValue));
        String base_path = params.get("base_path");
        Configuration config = new Configuration();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        params.forEach((k, v) -> config.set(k, v));
        return new HudiMetaClient(base_path, config);
    }

    public HudiMetaClient(String base_path, Configuration conf) {
        this.basePath = base_path;
        this.configuration = conf;
    }

    private static List<HudiMeta.Column> getAllColumns(Schema schema) {
        List<HudiMeta.Column> columns = new ArrayList<>();
        List<Schema.Field> allHudiColumns = schema.getFields();
        for (Schema.Field field : allHudiColumns) {
            String type = HudiColumnTypeConverter.fromHudiTypeToHiveTypeString(field.schema());
            columns.add(HudiMeta.Column.newBuilder().setName(field.name()).setType(type).build());
        }
        return columns;
    }

    @Override
    public byte[] getPartitionPaths() {
        HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(configuration);
        boolean useFileListingFromMetadata = true;
        List<String> paths = FSUtils.getAllPartitionPaths(engineContext, basePath, useFileListingFromMetadata, false);
        HudiMeta.PartitionPaths partitions = HudiMeta.PartitionPaths.newBuilder().addAllPaths(paths).build();
        return partitions.toByteArray();
    }

    @Override
    public byte[] getTable() throws Exception {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(configuration)
                .setBasePath(basePath)
                .setLoadActiveTimelineOnLoad(false)
                .build();

        HoodieTableConfig tableConfig = metaClient.getTableConfig();
        TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
        Schema schema = HoodieAvroUtils.createHoodieWriteSchema(schemaResolver.getTableAvroSchema());
        List<HudiMeta.Column> columns = getAllColumns(schema);
        List<String> partitionFields = new ArrayList<>();
        if (tableConfig.getPartitionFields().isPresent()) {
            partitionFields.addAll(Arrays.asList(tableConfig.getPartitionFields().get()));
        }

        HudiMeta.HudiTable table = HudiMeta.HudiTable.newBuilder()
//                .setHiveDbName(tableConfig.getDatabaseName())
//                .setHiveTableName(tableConfig.getTableName())
                .addAllColumns(columns)
                .addAllPartitionColumnName(partitionFields)
                .build();

        return table.toByteArray();
    }

    @Override
    public byte[] getFilesInPartition(String partitionName) throws IOException {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(configuration)
                .setBasePath(basePath)
                .build();
        metaClient.reloadActiveTimeline();
        HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
        Option<HoodieInstant> latestInstant = timeline.lastInstant();
        HudiMeta.HudiFileSlices.Builder builder = HudiMeta.HudiFileSlices.newBuilder();

        String globPath = String.format("%s/%s/*", metaClient.getBasePath(), partitionName);
        List<FileStatus> statuses = FSUtils.getGlobStatusExcludingMetaFolder(metaClient.getRawFs(), new Path(globPath));
        HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(metaClient,
                timeline, statuses.toArray(new FileStatus[0]));
        String queryInstant = latestInstant.get().getTimestamp();
        Iterator<FileSlice> hoodieFileSliceIterator = fileSystemView
                .getLatestMergedFileSlicesBeforeOrOn(partitionName, queryInstant).iterator();

        builder.setInstant(queryInstant);
        while (hoodieFileSliceIterator.hasNext()) {
            FileSlice fileSlice = hoodieFileSliceIterator.next();
            Optional<HoodieBaseFile> baseFile = fileSlice.getBaseFile().toJavaOptional();
            String fileName = baseFile.map(BaseFile::getFileName).orElse("");
            long fileLength = baseFile.map(BaseFile::getFileLen).orElse(-1L);
            List<String> logs = fileSlice.getLogFiles().map(HoodieLogFile::getFileName).collect(Collectors.toList());
            builder.addFileSlices(HudiMeta.HudiFileSlices.FileSlice.newBuilder()
                    .setBaseFileName(fileName)
                    .setBaseFileLength(fileLength)
                    .addAllDeltaLogs(logs)
                    .build());
        }
        return builder.build().toByteArray();
    }
}
