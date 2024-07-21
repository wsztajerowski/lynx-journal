package pl.wsztajerowski.journal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.lang.Integer.toHexString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static pl.wsztajerowski.journal.JournalHeader.*;
import static pl.wsztajerowski.journal.RecordHeader.RECORD_PREFIX;

public class JournalTestUtils {
    public static CharSequence journalHeaderPrefixInHexString() {
        return toHexString(JOURNAL_PREFIX).toUpperCase();
    }

    public static CharSequence journalSchemaVersionInHexString() {
        return toUpperCaseHexString(SCHEMA_VERSION);
    }

    public static CharSequence recordHeaderPrefixInHexString() {
        return toHexString(RECORD_PREFIX).toUpperCase();
    }

    public static CharSequence toUpperCaseHexString(int value) {
        return "%08x".formatted(value).toUpperCase();
    }

    public static ByteBuffer wrapInByteBuffer(String content) {
        return ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
    }

    public static String readAsUtf8(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.capacity()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }

    public static long firstRecordOffset() {
        return JOURNAL_HEADER_SIZE_IN_BYTES;
    }
}
