package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;

public class JournalByteBuffer {
    private final ByteBuffer byteBuffer;
    private final ByteBuffer contentBuffer;
    private final ByteBuffer headerBuffer;

    JournalByteBuffer(ByteBuffer byteBuffer, int headerSize) {
        this.byteBuffer = byteBuffer;
        this.contentBuffer = byteBuffer.slice(headerSize, byteBuffer.capacity()-headerSize);
        this.headerBuffer = byteBuffer.slice(0, headerSize);
    }

    public ByteBuffer getContentBuffer(){
        return contentBuffer;
    }

    ByteBuffer getHeaderBuffer(){
        return headerBuffer;
    }

    ByteBuffer getWritableBuffer(){
        return this.byteBuffer
            .duplicate()
            .limit(headerBuffer.capacity() + contentBuffer.limit())
            .rewind();
    }
}
