package org.byconity.common;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public abstract class ArrowReaderBuilder {
    /// public static ArrowReaderBuilder(byte[]) { return new(); }

    public void initStream(long streamAddress) {
        ArrowArrayStream stream = null;
        try {
            stream = ArrowArrayStream.wrap(streamAddress);
            ArrowReader reader = build();
            Data.exportArrayStream(getAllocator(), reader, stream);
        } finally {
            stream.close();
        }
    }

    public abstract ArrowReader build();

    public abstract BufferAllocator getAllocator();
}
