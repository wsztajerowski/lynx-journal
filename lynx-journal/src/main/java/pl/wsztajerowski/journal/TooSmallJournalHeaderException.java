package pl.wsztajerowski.journal;

public class TooSmallJournalHeaderException extends JournalException {
    public TooSmallJournalHeaderException() {
        super("Corrupted journal file - header size is too small");
    }
}
