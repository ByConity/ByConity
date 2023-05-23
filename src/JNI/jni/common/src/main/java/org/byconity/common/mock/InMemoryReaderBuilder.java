package org.byconity.common.mock;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.byconity.common.ArrowReaderBuilder;
import org.byconity.proto.HudiMeta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InMemoryReaderBuilder extends ArrowReaderBuilder {

    private BufferAllocator allocator;
    private int num_batch;
    private int batch_size;

    public static InMemoryReaderBuilder create(byte[] raw) throws InvalidProtocolBufferException {
        HudiMeta.Properties properties = HudiMeta.Properties.parseFrom(raw);
        Map<String, String> params = properties.getPropertiesList().stream().collect(
                Collectors.toMap(HudiMeta.Properties.KeyValue::getKey, HudiMeta.Properties.KeyValue::getValue));

        int num_batch = Integer.parseInt(params.get("num_batch"));
        int batch_size = Integer.parseInt(params.get("batch_size"));
        BufferAllocator allocator = new RootAllocator();
        return new InMemoryReaderBuilder(allocator, num_batch, batch_size);
    }

    public InMemoryReaderBuilder(BufferAllocator allocator, int num_batch, int batch_size) {
        this.allocator = allocator;
        this.num_batch = num_batch;
        this.batch_size = batch_size;
    }

    @Override
    public ArrowReader build() {
        final Schema schema = new Schema(Arrays.asList(Field.nullable("ints", new ArrowType.Int(32, true)),
                Field.nullable("strs", new ArrowType.Utf8())));
        final List<ArrowRecordBatch> batches = new ArrayList<>();
        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        final IntVector ints = (IntVector) root.getVector(0);
        final VarCharVector strs = (VarCharVector) root.getVector(1);
        VectorUnloader unloader = new VectorUnloader(root);

        for (int batch = 0; batch < num_batch; batch++) {
            root.allocateNew();
            for (int i = 0; i < batch_size; i++) {
                ints.setSafe(i, i);
                strs.setSafe(i, Integer.toString(i).getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(batch_size);
            batches.add(unloader.getRecordBatch());
        }
        return new InMemoryReader(allocator, schema, batches);
    }

    @Override
    public BufferAllocator getAllocator() {
        return allocator;
    }
}

class InMemoryReader extends ArrowReader {
    final static BufferAllocator allocator = new RootAllocator();
    private final Schema schema;
    private final List<ArrowRecordBatch> batches;
    private int nextBatch;

    public InMemoryReader(BufferAllocator allocator, Schema schema, List<ArrowRecordBatch> batches) {
        super(allocator);
        this.schema = schema;
        this.batches = batches;
        this.nextBatch = 0;
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        if (nextBatch < batches.size()) {
            VectorLoader loader = new VectorLoader(getVectorSchemaRoot());
            loader.load(batches.get(nextBatch++));
            return true;
        }

        return false;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        try {
            AutoCloseables.close(batches);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected Schema readSchema() throws IOException {
        return schema;
    }
}