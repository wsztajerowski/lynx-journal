package pl.wsztajerowski.journal;

import java.nio.ByteBuffer;

public class JournalHeader {
    public static final int JOURNAL_PREFIX = 0xCAFEBABE;
    public static final int SCHEMA_VERSION = 0x0FF1CE01;
    public static final int JOURNAL_HEADER_SIZE_IN_INTS = 2;
    public static final int JOURNAL_HEADER_SIZE_IN_BYTES = 4 * JOURNAL_HEADER_SIZE_IN_INTS;

    public static ByteBuffer createPopulatedHeader() {
        var buffer = ByteBuffer.allocate(8);
        buffer.putInt(JournalHeader.JOURNAL_PREFIX);
        buffer.putInt(JournalHeader.SCHEMA_VERSION);
        buffer.flip();
        return buffer;
    }
}
