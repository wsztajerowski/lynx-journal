package pl.wsztajerowski.journal.exceptions;

public class UnsupportedJournalVersion extends JournalException {
    public UnsupportedJournalVersion(int schemaVersion) {
        super("Unsupported schema version: %08x".formatted(schemaVersion));
    }
}
