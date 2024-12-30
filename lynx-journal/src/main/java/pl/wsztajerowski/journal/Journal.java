package pl.wsztajerowski.journal;

import pl.wsztajerowski.journal.records.Record;
import pl.wsztajerowski.journal.records.RecordReadChannel;
import pl.wsztajerowski.journal.records.RecordWriteChannel;
import pl.wsztajerowski.journal.records.RecordWriteTask;

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

public class Journal implements AutoCloseable {
    private static final int NUMBER_OF_INTS_IN_HEADER = 2;
    static final int JOURNAL_PREFIX = 0xCAFEBABE;
    static final int SCHEMA_VERSION_V1 = 0x0FF1CE01;
    static final List<Integer> SUPPORTED_SCHEMA_VERSIONS = List.of(SCHEMA_VERSION_V1);
    private final RecordReadChannel readChannel;
    private final RecordWriteChannel writeChannel;

    private final ExecutorService writeChannelExecutor = Executors.newSingleThreadExecutor();
    private final BlockingQueue<RecordWriteTask> writingQueue;

    Journal(RecordReadChannel readChannel, RecordWriteChannel writeChannel, BlockingQueue<RecordWriteTask> writingQueue) {
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;
        this.writingQueue = writingQueue;
        writeChannelExecutor.submit(writeChannel);
    }

    static int journalHeaderLength() {
        return NUMBER_OF_INTS_IN_HEADER * Integer.BYTES;
    }

    public static Journal open(Path path, boolean truncateFile) {
        try {
            // FIXME: FileChannel.open() with StandardOption.CREATE throws NoSuchFileException
            if (Files.notExists(path)) {
                Files.createFile(path);
                return initJournal(path);
            }

            long journalFileSize = Files.size(path);
            if (truncateFile || journalFileSize == 0) {
                return initJournal(path);
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
        BlockingQueue<RecordWriteTask> writingQueue = new LinkedBlockingQueue<>();
        return new Journal(RecordReadChannel.open(path), RecordWriteChannel.open(path, writingQueue), writingQueue);
    }

    private static Journal initJournal(Path path) {
        try {
            Files.write(path, toByteArray(Journal.JOURNAL_PREFIX, Journal.SCHEMA_VERSION_V1));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        BlockingQueue<RecordWriteTask> writingQueue = new LinkedBlockingQueue<>();
        return new Journal(RecordReadChannel.open(path), RecordWriteChannel.open(path, writingQueue), writingQueue);
    }

    public void close() throws IOException {
        try {
            readChannel.close();
            writeChannelExecutor.shutdown();
        } finally {
            writeChannel.close();
        }
    }

    public Record readRecord(ByteBuffer destination, Location location) {
        return readChannel.read(destination, location);
    }

    public ByteBuffer read(ByteBuffer destination, Location location) {
        return readRecord(destination, location)
            .buffer();
    }

    public Location write(JournalByteBuffer buffer) {
        CompletableFuture<Location> future = new CompletableFuture<>();
        RecordWriteTask task = new RecordWriteTask(buffer.getWritableBuffer(), future);
        while (!writingQueue.offer(task)) {
            Thread.onSpinWait();
        }
        return future.join();
    }
}
