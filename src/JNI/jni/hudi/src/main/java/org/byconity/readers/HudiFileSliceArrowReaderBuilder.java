package org.byconity.readers;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.byconity.common.ArrowReaderBuilder;
import org.byconity.hudi.HiveArrowUtils;
import org.byconity.proto.HudiMeta;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class HudiFileSliceArrowReaderBuilder extends ArrowReaderBuilder {

    private static final Logger LOG = LogManager.getLogger(HudiFileSliceArrowReaderBuilder.class);
    private BufferAllocator allocator;
    private final int fetch_size;
    private final String basePath;
    private final String[] hiveColumnNames;
    private final String[] hiveColumnTypes;
    private final String[] requiredFields;
    private int[] requiredColumnIds;
    private final String instantTime;
    private final String dataFilePath;
    private final long dataFileLength;
    private final String[] deltaFilePaths;
    private final String serde;
    private final String inputFormat;

    private static String getOrThrow(Map<String, String> map, String key) {
        return Optional.ofNullable(map.get(key)).orElseThrow(() -> new NoSuchElementException(key + " not found"));
    }

    public static HudiFileSliceArrowReaderBuilder create(byte[] raw) throws InvalidProtocolBufferException {
        BufferAllocator allocator = new RootAllocator();
        HudiMeta.Properties properties = HudiMeta.Properties.parseFrom(raw);
        Map<String, String> params = properties.getPropertiesList().stream().collect(
                Collectors.toMap(HudiMeta.Properties.KeyValue::getKey, HudiMeta.Properties.KeyValue::getValue));
        return new HudiFileSliceArrowReaderBuilder(allocator, params);
    }

    public HudiFileSliceArrowReaderBuilder(BufferAllocator allocator, Map<String, String> params) {
        this.allocator = allocator;
        this.fetch_size = Integer.parseInt(getOrThrow(params, "fetch_size"));
        this.basePath = getOrThrow(params,"base_path");
        this.hiveColumnNames = getOrThrow(params, "hive_column_names").split(",");
        this.hiveColumnTypes = getOrThrow(params, "hive_column_types").split("#");
        this.requiredFields = getOrThrow(params, "required_fields").split(",");
        parseRequiredType();
        this.instantTime = getOrThrow(params, "instant_time");
        this.dataFilePath = params.getOrDefault("data_file_path", "");
        this.dataFileLength = Long.parseLong(params.getOrDefault("data_file_length", "0"));
        this.deltaFilePaths = params.getOrDefault("delta_file_paths", "").split(",");
        this.serde = getOrThrow(params, "serde");
        this.inputFormat = getOrThrow(params, "input_format");
        params.forEach((k, v) -> LOG.debug("key=" + k + ", value=" + v));
    }

    private void parseRequiredType()
    {
        HashMap<String, Integer> hiveColumnNameToIndex = new HashMap<>();
        for (int i = 0; i < hiveColumnNames.length; i++) {
            hiveColumnNameToIndex.put(hiveColumnNames[i], i);
        }

        this.requiredColumnIds = new int[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++)
        {
            requiredColumnIds[i] = hiveColumnNameToIndex.get(requiredFields[i]);
        }
    }

    private RecordReader<NullWritable, ArrayWritable> initReader(JobConf jobConf, Properties properties) throws Exception {
        // dataFileLenth==-1 or dataFilePath == "" means logs only scan
        boolean log_file_only = dataFileLength == -1 || dataFilePath.isEmpty();
        String realtimePath = log_file_only ? deltaFilePaths[0] : dataFilePath;
        long realtimeLength = log_file_only ? 0 : dataFileLength;
        Path path = new Path(realtimePath);
        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new).collect(toList());
        FileSplit hudiSplit = new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, instantTime, false, Option.empty());
        //System.out.println(hudiSplit.toString());
        InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, inputFormat);
        return (RecordReader<NullWritable, ArrayWritable>) inputFormatClass.getRecordReader(hudiSplit, jobConf, Reporter.NULL);
    }

    private JobConf makeJobConf(Properties properties) {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);
        jobConf.setBoolean("hive.io.file.read.all.columns", false);
        properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));
        return jobConf;
    }

    private Properties makeProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids", Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        properties.setProperty("hive.io.file.readcolumn.names", String.join(",", this.requiredFields));
        properties.setProperty("columns", Arrays.stream(this.hiveColumnNames).collect(Collectors.joining(",")));
        properties.setProperty("columns.types", Arrays.stream(this.hiveColumnTypes).collect(Collectors.joining(",")));
        properties.setProperty("serialization.lib", this.serde);
        return properties;
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name)
            throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                "expected STRUCT: %s", inspector.getCategory());
        return (StructObjectInspector) inspector;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("basePath: ");
        sb.append(basePath);
        sb.append("\n");
        sb.append("hiveColumnNames: ");
        sb.append(Arrays.toString(hiveColumnNames));
        sb.append("\n");
        sb.append("hiveColumnTypes: ");
        sb.append(Arrays.toString(hiveColumnTypes));
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("instantTime: ");
        sb.append(instantTime);
        sb.append("\n");
        sb.append("deltaFilePaths: ");
        sb.append(Arrays.toString(deltaFilePaths));
        sb.append("\n");
        sb.append("dataFilePath: ");
        sb.append(dataFilePath);
        sb.append("\n");
        sb.append("dataFileLenth: ");
        sb.append(dataFileLength);
        sb.append("\n");
        sb.append("serde: ");
        sb.append(serde);
        sb.append("\n");
        sb.append("inputFormat: ");
        sb.append(inputFormat);
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public ArrowReader build() throws IOException {
        try
        {
            Properties properties = makeProperties();
            JobConf jobConf = makeJobConf(properties);
            RecordReader<NullWritable, ArrayWritable> reader = initReader(jobConf, properties);
            Deserializer deserializer = getDeserializer(jobConf, properties, serde);
            StructObjectInspector rowInspector = getTableObjectInspector(deserializer);
            StructField[] requiredStructFields = new StructField[requiredFields.length];
            for (int i = 0; i < requiredFields.length; i++) {
                requiredStructFields[i] = rowInspector.getStructFieldRef(requiredFields[i]);
            }

            Schema schema = HiveArrowUtils.hiveToArrowSchema(requiredStructFields);
            return new HudiFileSliceArrowReader(allocator, schema, fetch_size, reader, rowInspector, requiredStructFields, deserializer);
        } catch (Exception e) {
            LOG.error("Failed to open the hudi MOR slice reader.", e);
            throw new IOException("Failed to open the hudi MOR slice reader.", e);
        }
    }

    @Override
    public BufferAllocator getAllocator() {
        return allocator;
    }
}
