package pl.wsztajerowski.journal.exceptions;

public class TooSmallJournalHeader extends JournalException {
    public TooSmallJournalHeader() {
        super("Corrupted journal file - header size is too small");
    }
}
