package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalRuntimeIOException;

import java.nio.ByteBuffer;

import static pl.wsztajerowski.journal.records.ChecksumCalculator.computeChecksum;
import static pl.wsztajerowski.journal.records.RecordHeader.RECORD_PREFIX;

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

    private void prepareRecordHeaderBufferToWrite(int variableSize, int checksum) {
        headerBuffer.putInt(0, RECORD_PREFIX);
        headerBuffer.putInt(4, variableSize);
        headerBuffer.putInt(8, checksum);
        headerBuffer.rewind();
    }

    public ByteBuffer getWritableBuffer(){
        int variableSize = contentBuffer.remaining();
        if (variableSize == 0) {
            throw new JournalRuntimeIOException("Buffer contains no data to write");
        }
        var checksum = computeChecksum(contentBuffer);
        prepareRecordHeaderBufferToWrite(variableSize, checksum);
        return this.byteBuffer
//            .duplicate()
            .limit(headerBuffer.capacity() + this.contentBuffer.limit())
            .rewind();
    }

    public JournalByteBuffer clear() {
        byteBuffer.clear();
        headerBuffer.clear();
        contentBuffer.clear();
        return this;
    }
}
