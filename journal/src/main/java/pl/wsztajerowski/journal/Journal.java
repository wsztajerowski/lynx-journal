package pl.wsztajerowski.journal;

import pl.wsztajerowski.journal.records.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.*;

import static pl.wsztajerowski.journal.BytesUtils.fromByteArray;
import static pl.wsztajerowski.journal.BytesUtils.toByteArray;

public class Journal implements AutoCloseable {
    public static final int BATCH_SIZE = 4096;
    private static final int NUMBER_OF_INTS_IN_HEADER = 2;
    static final int JOURNAL_PREFIX = 0xCAFEBABE;
    static final int SCHEMA_VERSION_V1 = 0x0FF1CE01;
    static final List<Integer> SUPPORTED_SCHEMA_VERSIONS = List.of(SCHEMA_VERSION_V1);

    private final RecordReadChannel readChannel;

    private final DoubleBatch doubleBatch;

    Journal(RecordReadChannel readChannel, DoubleBatch doubleBatch) {
        this.readChannel = readChannel;
        this.doubleBatch = doubleBatch;
    }

    static int journalHeaderLength() {
        return NUMBER_OF_INTS_IN_HEADER * Integer.BYTES;
    }

    public static Journal open(Path path, boolean truncateFile) {
        return open(path, truncateFile, BATCH_SIZE);
    }

    public static Journal open(Path path, boolean truncateFile, int batchSize) {
        try {
            long journalFileSize = Files.size(path);
            if (truncateFile || journalFileSize == 0) {
                return createEmptyJournal(path, batchSize);
            }

            if (journalFileSize > 0 && journalFileSize < journalHeaderLength()) {
                throw new TooSmallJournalHeaderException();
            }

            // v01 journal header format: [ int prefix, int schemaVersion ]
            try (FileInputStream inputStream = new FileInputStream(path.toFile())) {
                byte[] header = inputStream.readNBytes(journalHeaderLength());
                if (header.length < journalHeaderLength()) {
                    throw new TooSmallJournalHeaderException();
                }
                int headerPrefix = fromByteArray(header, 0);
                if (headerPrefix != JOURNAL_PREFIX) {
                    throw new InvalidJournalHeaderException(headerPrefix);
                }
                int schemaVersion = fromByteArray(header, 1);
                if (!SUPPORTED_SCHEMA_VERSIONS.contains(schemaVersion)) {
                    throw new UnsupportedJournalVersionException(schemaVersion);
                }
            }

            return initJournal(path, batchSize);
        } catch (IOException e) {
            throw new UncheckedIOException("Error during opening a Journal", e);
        }
    }

    private static Journal createEmptyJournal(Path path, int batchSize) throws IOException {
        Files.write(path, toByteArray(Journal.JOURNAL_PREFIX, Journal.SCHEMA_VERSION_V1), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        return initJournal(path, batchSize);
    }

    private static Journal initJournal(Path path, int batchSize) throws IOException {
        DoubleBatch doubleBatch = DoubleBatch.open(path, batchSize);
        return new Journal(RecordReadChannel.open(path), doubleBatch);
    }

    public void close() throws IOException {
        try {
            doubleBatch.close();
        } finally {
            readChannel.close();
        }
    }

    public Location write(JournalByteBuffer buffer) {
        ByteBuffer writableBuffer = buffer.getWritableBuffer();
        long location = doubleBatch.write(writableBuffer, true);
        return new Location(location);
    }

    public Location writeAsync(JournalByteBuffer buffer) {
        ByteBuffer writableBuffer = buffer.getWritableBuffer();
        long location = doubleBatch.write(writableBuffer, false);
        return new Location(location);
    }

    public ByteBuffer read(JournalByteBuffer destination, Location location) {
        return readChannel.read(destination, location).buffer();
    }

}
