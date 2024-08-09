package pl.wsztajerowski.journal;

public class InvalidJournalHeaderException extends JournalException {
    public InvalidJournalHeaderException(int journalPrefix) {
        super("Invalid journal header prefix: [  %08x ]".formatted(journalPrefix));
    }
}
