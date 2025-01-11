package pl.wsztajerowski.journal.records;

public class ByteBufferFactory {
    public static JournalByteBuffer newJournalBuffer(int position, int limit, int capacity) {
        JournalByteBuffer buffer = JournalByteBufferFactory.createJournalByteBuffer(capacity);
        buffer.getContentBuffer()
            .position(position)
            .limit(limit);
        return buffer;
    }
}
