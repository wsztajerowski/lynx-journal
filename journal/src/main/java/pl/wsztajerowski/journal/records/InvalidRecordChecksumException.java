package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalException;

public class InvalidRecordChecksumException extends JournalException {
    public InvalidRecordChecksumException(long calculatedChecksum, long expectedChecksum) {
        super("Invalid record checksum: %d, expected %d".formatted(calculatedChecksum, expectedChecksum));
    }
}
