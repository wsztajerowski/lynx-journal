package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalException;

public class NotEnoughSpaceInBufferException extends JournalException {
    public NotEnoughSpaceInBufferException(int actualSize, int expectedSize) {
        super("Not enough space in buffer - expected: %d, actual: %d".formatted(expectedSize, actualSize));
    }
}
