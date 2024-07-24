package pl.wsztajerowski.journal;

public class HexTestUtils {
    public static CharSequence toUpperCaseHexString(int value) {
        return "%08x".formatted(value).toUpperCase();
    }
}
