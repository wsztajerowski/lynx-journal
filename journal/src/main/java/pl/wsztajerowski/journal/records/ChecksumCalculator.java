package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class ChecksumCalculator {
    private static Checksum newChecksum() {
        return new CRC32C();
    }

    public static int computeChecksum(ByteBuffer buffer) {
        Checksum checksum = newChecksum();
        buffer.mark();
        checksum.update(buffer);
        buffer.reset();
        return Long.valueOf(checksum.getValue()).intValue();
    }

    public static int computeChecksum(int variable) {
        Checksum checksum = newChecksum();
        checksum.update(variable);
        return Long.valueOf(checksum.getValue()).intValue();
    }

    public static int computeChecksum(String variable) {
        Checksum checksum = newChecksum();
        checksum.update(variable.getBytes());
        return Long.valueOf(checksum.getValue()).intValue();
    }
}
