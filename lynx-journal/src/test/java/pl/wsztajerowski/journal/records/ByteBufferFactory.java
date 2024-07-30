package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;

public class ByteBufferFactory {
    public static ByteBuffer newByteBuffer(int position, int limit, int capacity) {
        return ByteBuffer.allocate(capacity)
            .position(position)
            .limit(limit);
    }
}
