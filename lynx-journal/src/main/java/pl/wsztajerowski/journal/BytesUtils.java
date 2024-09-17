package pl.wsztajerowski.journal;

public class BytesUtils {
    public static byte[] toByteArray(int value, int value2) {
        return new byte[] {
            (byte)(value >> 24),
            (byte)(value >> 16),
            (byte)(value >> 8),
            (byte)value,
            (byte)(value2 >> 24),
            (byte)(value2 >> 16),
            (byte)(value2 >> 8),
            (byte)value2
        };
    }

    public static byte[] toByteArray(int value) {
        return new byte[] {
            (byte)(value >> 24),
            (byte)(value >> 16),
            (byte)(value >> 8),
            (byte)value
        };
    }

    public static int fromByteArray(byte[] bytes, int intIndex) {
        if (bytes.length < intIndex*4+4){
            throw new IllegalArgumentException("Not enough bytes for index " + intIndex);
        }
        return ((bytes[intIndex*4] & 0xFF) << 24) |
            ((bytes[intIndex*4+1] & 0xFF) << 16) |
            ((bytes[intIndex*4+2] & 0xFF) << 8 ) |
            ((bytes[intIndex*4+3] & 0xFF));
    }
}
