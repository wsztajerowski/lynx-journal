package pl.wsztajerowski.journal;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

public class BytesTestUtils {
    public static CharSequence toUpperCaseHexString(int value) {
        return "%08x".formatted(value).toUpperCase();
    }
    public static CharSequence toUpperCaseHexString(long value) {
        return "%016x".formatted(value).toUpperCase();
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

    public static byte[] longToBytes(long variable) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte)(variable & 0xFF);
            variable >>= Byte.SIZE;
        }
        return result;
    }

    public static long bytesToLong(final byte[] bytes) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }
}
