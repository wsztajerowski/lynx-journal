package pl.wsztajerowski.journal.exceptions;

public class InvalidRecordChecksum extends JournalException {
    public InvalidRecordChecksum(long calculatedChecksum, long expectedChecksum) {
        super("Invalid record checksum: %d, expected %d".formatted(calculatedChecksum, expectedChecksum));
    }
}
