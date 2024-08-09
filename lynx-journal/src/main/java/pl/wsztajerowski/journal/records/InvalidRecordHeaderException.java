package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalException;

public class InvalidRecordHeaderException extends JournalException {
    public InvalidRecordHeaderException(String message) {
        super(message);
    }

    public static InvalidRecordHeaderException invalidRecordHeaderPrefix(int prefix) {
        return new InvalidRecordHeaderException("Invalid record header prefix - actual: [ %08x ], expected: [ %08x ]".formatted(prefix, RecordHeader.RECORD_PREFIX));
    }

    public static InvalidRecordHeaderException invalidRecordVariableSize(int variableSize) {
        return new InvalidRecordHeaderException("Record's variable size must be greater than 0 - actual: %d".formatted(variableSize));
    }
}
