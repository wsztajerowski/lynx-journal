package pl.wsztajerowski.journal.exceptions;

public class InvalidJournalHeader extends JournalException {
    public InvalidJournalHeader(int journalPrefix) {
        super("Invalid journal header prefix: [  %08x ]".formatted(journalPrefix));
    }
}
