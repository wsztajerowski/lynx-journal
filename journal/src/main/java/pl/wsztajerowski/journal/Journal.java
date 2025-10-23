package pl.wsztajerowski.journal;

import pl.wsztajerowski.journal.records.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;

import static pl.wsztajerowski.journal.BytesUtils.fromByteArray;
import static pl.wsztajerowski.journal.BytesUtils.toByteArray;

/**
 * Założenia do nowej implementacji:
 * - dwa batch'e z stały rozmiarem i DirectByteBuffer pod spodem
 * - Consumer przepina AtomicReference pomiędzy batchami
 * - Client czeka na condition z batcha
 * - producent kopiuje dane z byte buffera clienta do bufora batch'a
 *
 */
public class Journal implements AutoCloseable {
    public static final int BATCH_SIZE = 4096;
    private static final int NUMBER_OF_INTS_IN_HEADER = 2;
    static final int JOURNAL_PREFIX = 0xCAFEBABE;
    static final int SCHEMA_VERSION_V1 = 0x0FF1CE01;
    static final List<Integer> SUPPORTED_SCHEMA_VERSIONS = List.of(SCHEMA_VERSION_V1);

    private final RecordReadChannel readChannel;
    private final RecordWriteChannel writeChannel;

    private final ExecutorService writeChannelExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "write-channel"));
    private final DoubleBatch doubleBatch;

    Journal(RecordReadChannel readChannel, RecordWriteChannel writeChannel, DoubleBatch doubleBatch) {
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;
        this.doubleBatch = doubleBatch;
        writeChannelExecutor.submit(writeChannel);
    }

    static int journalHeaderLength() {
        return NUMBER_OF_INTS_IN_HEADER * Integer.BYTES;
    }

    public static Journal open(Path path, boolean truncateFile) {
        return open(path, truncateFile, BATCH_SIZE);
    }

    public static Journal open(Path path, boolean truncateFile, int batchSize) {
        try {
            // FIXME: FileChannel.open() with StandardOption.CREATE throws NoSuchFileException
            if (Files.notExists(path)) {
                Files.createFile(path);
                return createEmptyJournal(path, batchSize);
            }

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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return initJournal(path, batchSize);
    }

    private static Journal createEmptyJournal(Path path, int batchSize) {
        try {
            Files.write(path, toByteArray(Journal.JOURNAL_PREFIX, Journal.SCHEMA_VERSION_V1));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return initJournal(path, batchSize);
    }

    private static Journal initJournal(Path path, int batchSize) {
        DoubleBatch doubleBatch = new DoubleBatch(batchSize);
        RecordWriteChannel recordWriteChannel = RecordWriteChannel.open(path, doubleBatch);
        long initJournalFilePosition = recordWriteChannel.getCurrentPosition();
        doubleBatch.initVirtualPosition(initJournalFilePosition);
        return new Journal(RecordReadChannel.open(path), recordWriteChannel, doubleBatch);
    }

    public void close() throws IOException {
        try {
            try {
                writeChannelExecutor.shutdown();
                readChannel.close();
            } finally {
                writeChannel.close();
            }
        } finally {
            try {
                if (!writeChannelExecutor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    writeChannelExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                writeChannelExecutor.shutdownNow();
            }
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

//    public ByteBuffer readAsync(JournalByteBuffer destination, Location location) {
//        return Optional.ofNullable(doubleBatch.get(location.offset()))
//            .map(buffer -> {
//                ByteBuffer contentBuffer = destination.getContentBuffer();
//                contentBuffer.put(contentBuffer.position(),buffer, recordHeaderLength(), buffer.limit()-recordHeaderLength());
//                contentBuffer.limit(buffer.limit()-recordHeaderLength());
//                return contentBuffer;
//            })
//            .orElseGet(() -> readChannel.read(destination, location).buffer());
//    }
}
