package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalException;

public class InvalidRecordHeaderException extends JournalException {
    public InvalidRecordHeaderException(int prefix, int variableSize) {
        super("Invalid record header format - actual header [ %08x, %08x ]".formatted(prefix, variableSize));
    }
}
