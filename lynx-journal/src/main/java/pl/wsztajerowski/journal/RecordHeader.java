package pl.wsztajerowski.journal;

import java.nio.ByteBuffer;

public record RecordHeader(int variableType, int variableSize){
    public static final int RECORD_PREFIX = 0xF0CACC1A;
    public static final int TYPE_BYTE_BUFFER = 100;
    public static final int RECORD_HEADER_SIZE_IN_INTS = 3;
    public static final int RECORD_HEADER_SIZE_IN_BYTES = 4 * RECORD_HEADER_SIZE_IN_INTS;

    public RecordHeader(int length){
        this(TYPE_BYTE_BUFFER, length);
    }

    public ByteBuffer getRecordHeaderBuffer() {
        var buffer = ByteBuffer.allocate(RECORD_HEADER_SIZE_IN_BYTES);
        buffer.putInt(RECORD_PREFIX);
        buffer.putInt(this.variableType()); // for future use
        buffer.putInt(this.variableSize());
        buffer.flip();
        return buffer;
    }
}
