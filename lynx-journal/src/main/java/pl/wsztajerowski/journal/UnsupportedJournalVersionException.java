package pl.wsztajerowski.journal;

public class UnsupportedJournalVersionException extends JournalException {
    public UnsupportedJournalVersionException(int schemaVersion) {
        super("Unsupported schema version: %08x".formatted(schemaVersion));
    }
}
