package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;

public class JournalByteBufferFactory {
    private JournalByteBufferFactory() {}

    public static JournalByteBuffer createJournalByteBuffer(int size) {
        int headerLength = RecordHeader.recordHeaderLength();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(headerLength + size);
        return new JournalByteBuffer(byteBuffer, headerLength);
    }
}
