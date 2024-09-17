package pl.wsztajerowski.journal;

import java.io.IOException;

public class JournalException extends RuntimeException {
    public JournalException(String message) {
        super(message);
    }

    public JournalException(String message, Throwable cause) {
        super(message, cause);
    }

    public JournalException(IOException cause) {
        super(cause);
    }
}
