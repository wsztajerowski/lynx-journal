package pl.wsztajerowski.journal;

public class JournalException extends RuntimeException {
    public JournalException(String message) {
        super(message);
    }

    public JournalException(String message, Throwable cause) {
        super(message, cause);
    }

    public JournalException(Exception cause) {
        super(cause);
    }
}
