package pl.wsztajerowski.journal;

import pl.wsztajerowski.journal.records.JournalByteBuffer;
import pl.wsztajerowski.journal.records.JournalByteBufferFactory;
import pl.wsztajerowski.journal.records.RecordHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.nio.charset.StandardCharsets.UTF_8;
import static pl.wsztajerowski.journal.BytesTestUtils.intToBytes;

public class FilesTestUtils {
    public static ByteBuffer wrapInByteBuffer(String content) {
        return ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
    }
    public static JournalByteBuffer wrapInJournalByteBuffer(String content) {
        byte[] contentBytes = content.getBytes(UTF_8);
        JournalByteBuffer journalByteBuffer = JournalByteBufferFactory.createJournalByteBuffer(contentBytes.length);
        ByteBuffer contentBuffer = journalByteBuffer.getContentBuffer();
        contentBuffer
            .put(contentBytes);
        contentBuffer.flip();
        return journalByteBuffer;
    }

    public static String readAsUtf8(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes, 0, buffer.remaining());
        return new String(bytes, UTF_8);
    }

    public static void appendToFile(Path filepath, String content) throws IOException {
        Files.writeString(filepath, content, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public static void appendToFile(Path filepath, byte[] content) throws IOException {
        Files.write(filepath, content, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public static void appendToFile(Path filepath, int content) throws IOException {
        Files.write(filepath, intToBytes(content), StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }
}
