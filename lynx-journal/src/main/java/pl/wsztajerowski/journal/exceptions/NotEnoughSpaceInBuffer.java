package pl.wsztajerowski.journal.exceptions;

public class NotEnoughSpaceInBuffer extends JournalException {
    public NotEnoughSpaceInBuffer(int actualSize, int expectedSize) {
        super("Not enough space in buffer - expected: %d, actual: %d".formatted(expectedSize, actualSize));
    }
}
