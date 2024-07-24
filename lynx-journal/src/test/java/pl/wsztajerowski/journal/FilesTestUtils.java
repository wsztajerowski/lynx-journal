package pl.wsztajerowski.journal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FilesTestUtils {
    public static ByteBuffer wrapInByteBuffer(String content) {
        return ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
    }

    public static String readAsUtf8(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        return new String(bytes, UTF_8);
    }
}
