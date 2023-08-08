package org.byconity.common;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;

public abstract class ArrowReaderBuilder {
    /// public static ArrowReaderBuilder(byte[]) { return new(); }

    public void initStream(long streamAddress) throws IOException {
        ArrowArrayStream stream = ArrowArrayStream.wrap(streamAddress);
        ArrowReader reader = build();
        Data.exportArrayStream(getAllocator(), reader, stream);
    }

    public abstract ArrowReader build() throws IOException;

    public abstract BufferAllocator getAllocator();

}
