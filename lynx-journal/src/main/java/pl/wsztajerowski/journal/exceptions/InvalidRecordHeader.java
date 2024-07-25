package pl.wsztajerowski.journal.exceptions;

public class InvalidRecordHeader extends JournalException {
    public InvalidRecordHeader() {
        super("Invalid record header format");
    }
}
