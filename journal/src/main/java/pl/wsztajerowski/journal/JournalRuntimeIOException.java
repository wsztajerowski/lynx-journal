package pl.wsztajerowski.journal;

import java.io.IOException;

public class JournalRuntimeIOException extends JournalException {
    public JournalRuntimeIOException(IOException cause) {
        super(cause);
    }

    public JournalRuntimeIOException(String message, IOException cause) {
        super(message, cause);
    }

    public JournalRuntimeIOException(String message) {
        super(message);
    }
}
