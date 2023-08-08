package org.byconity.readers;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.byconity.hudi.HiveArrowUtils;

import java.io.IOException;

public class HudiFileSliceArrowReader extends ArrowReader {
    private static final Logger LOG = LogManager.getLogger(HudiFileSliceArrowReaderBuilder.class);

    private Schema schema;
    private int fetch_size;
    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private StructField[] requiredStructFields;
    private Deserializer deserializer;

    protected HudiFileSliceArrowReader(BufferAllocator allocator, Schema schema, int fetch_size, RecordReader<NullWritable, ArrayWritable> reader, StructObjectInspector rowInspector, StructField[] requiredStructFields, Deserializer deserializer) {
        super(allocator);
        this.schema = schema;
        this.fetch_size = fetch_size;
        this.reader = reader;
        this.rowInspector = rowInspector;
        this.requiredStructFields = requiredStructFields;
        this.deserializer = deserializer;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        NullWritable key = reader.createKey();
        ArrayWritable value = reader.createValue();
        try {
            VectorSchemaRoot vectorSchemaRoot = getVectorSchemaRoot();
            vectorSchemaRoot.allocateNew();
            VectorUnloader unloader = new VectorUnloader(vectorSchemaRoot);

            int numRows = 0;
            for (; numRows < fetch_size; numRows++) {
                if (!reader.next(key, value)) {
                    break;
                }
                Object row = deserializer.deserialize(value);
                for (int i = 0; i < requiredStructFields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(row, requiredStructFields[i]);
                    if (fieldData == null)
                        vectorSchemaRoot.getVector(i).setNull(numRows);
                    else {
                        ObjectInspector inspector = requiredStructFields[i].getFieldObjectInspector();
                        HiveArrowUtils.setArrowFieldValue(vectorSchemaRoot.getVector(i), inspector, numRows, fieldData);
                    }
                }
            }
            vectorSchemaRoot.setRowCount(numRows);
            if (numRows > 0)
            {
                ArrowRecordBatch recordBatch = unloader.getRecordBatch();
                this.loadRecordBatch(recordBatch);
                return true;
            }
            return false;
        } catch (Exception e) {
            close();
            LOG.error("Failed to load next batch of hudi data", e);
            throw new IOException("Failed to load next batch of hudi data.", e);
        }
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        reader.close();
    }

    @Override
    protected Schema readSchema() throws IOException {
        return schema;
    }
}
