package pl.wsztajerowski.journal.exceptions;

public class InvalidJournalHeader extends JournalException {
    public InvalidJournalHeader() {
        super("Invalid journal header format");
    }
}
