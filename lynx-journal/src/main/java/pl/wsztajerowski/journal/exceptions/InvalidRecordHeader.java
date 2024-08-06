package pl.wsztajerowski.journal.exceptions;

public class InvalidRecordHeader extends JournalException {
    public InvalidRecordHeader(int prefix, int variableSize) {
        super("Invalid record header format - actual header [ %08x, %08x ]".formatted(prefix, variableSize));
    }
}
