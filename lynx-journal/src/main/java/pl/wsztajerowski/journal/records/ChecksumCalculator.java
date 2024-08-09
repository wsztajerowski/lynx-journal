package pl.wsztajerowski.journal.records;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class ChecksumCalculator {
    private static Checksum newChecksum() {
        return new CRC32C();
    }

    public static long computeChecksum(ByteBuffer buffer) {
        Checksum checksum = newChecksum();
        checksum.update(buffer.duplicate());
        return checksum.getValue();
    }

    public static long computeChecksum(int variable) {
        Checksum checksum = newChecksum();
        checksum.update(variable);
        return checksum.getValue();
    }

    public static long computeChecksum(String variable) {
        Checksum checksum = newChecksum();
        checksum.update(variable.getBytes());
        return checksum.getValue();
    }
}
