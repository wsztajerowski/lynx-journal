package pl.wsztajerowski.journal;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

public class BytesTestUtils {
    public static CharSequence toUpperCaseHexString(int value) {
        return "%08x".formatted(value).toUpperCase();
    }

    public static CharSequence toUtf8HexString(String value) {
        return HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] intToBytes(int value) {
        return new byte[] {
            (byte)(value >> 24),
            (byte)(value >> 16),
            (byte)(value >> 8),
            (byte)value};
    }
}
