package pl.wsztajerowski.journal;

import pl.wsztajerowski.journal.records.JournalByteBuffer;
import pl.wsztajerowski.journal.records.RecordReadChannel;
import pl.wsztajerowski.journal.records.RecordWriteChannel;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static pl.wsztajerowski.journal.BytesUtils.fromByteArray;
import static pl.wsztajerowski.journal.BytesUtils.toByteArray;
import static pl.wsztajerowski.journal.records.RecordHeader.recordHeaderLength;

public class Journal implements AutoCloseable {
    private static final int NUMBER_OF_INTS_IN_HEADER = 2;
    static final int JOURNAL_PREFIX = 0xCAFEBABE;
    static final int SCHEMA_VERSION_V1 = 0x0FF1CE01;
    static final List<Integer> SUPPORTED_SCHEMA_VERSIONS = List.of(SCHEMA_VERSION_V1);

    private final RecordReadChannel readChannel;
    private final RecordWriteChannel writeChannel;

    private final ExecutorService writeChannelExecutor = Executors.newSingleThreadExecutor();
    private final ConcurrentNavigableMap<Long, ByteBuffer> writingQueue;
    private final AtomicLong virtualPosition;

    Journal(RecordReadChannel readChannel, RecordWriteChannel writeChannel, ConcurrentNavigableMap<Long, ByteBuffer> writingQueue, long initJournalFilePosition) {
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;
        this.writingQueue = writingQueue;
        writeChannelExecutor.submit(writeChannel);
        virtualPosition = new AtomicLong(initJournalFilePosition);
    }

    static int journalHeaderLength() {
        return NUMBER_OF_INTS_IN_HEADER * Integer.BYTES;
    }

    public static Journal open(Path path, boolean truncateFile) {
        try {
            // FIXME: FileChannel.open() with StandardOption.CREATE throws NoSuchFileException
            if (Files.notExists(path)) {
                Files.createFile(path);
                return createEmptyJournal(path);
            }

            long journalFileSize = Files.size(path);
            if (truncateFile || journalFileSize == 0) {
                return createEmptyJournal(path);
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
        return initJournal(path);
    }

    private static Journal createEmptyJournal(Path path) {
        try {
            Files.write(path, toByteArray(Journal.JOURNAL_PREFIX, Journal.SCHEMA_VERSION_V1));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return initJournal(path);
    }

    private static Journal initJournal(Path path) {
        ConcurrentNavigableMap<Long, ByteBuffer> writingQueue = new ConcurrentSkipListMap<>();
        RecordWriteChannel recordWriteChannel = RecordWriteChannel.open(path, writingQueue);
        long initJournalFilePosition = recordWriteChannel.getCurrentPosition();
        return new Journal(RecordReadChannel.open(path), recordWriteChannel, writingQueue, initJournalFilePosition);
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

    public ByteBuffer read(JournalByteBuffer destination, Location location) {
        while (writingQueue.containsKey(location.offset())) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new JournalException(e);
            }
        }
        return readChannel.read(destination, location).buffer();
    }

    public ByteBuffer readAsync(JournalByteBuffer destination, Location location) {
        return Optional.ofNullable(writingQueue.get(location.offset()))
            .map(buffer -> {
                ByteBuffer contentBuffer = destination.getContentBuffer();
                contentBuffer.put(contentBuffer.position(),buffer, recordHeaderLength(), buffer.limit()-recordHeaderLength());
                contentBuffer.limit(buffer.limit()-recordHeaderLength());
                return contentBuffer;
            })
            .orElseGet(() -> readChannel.read(destination, location).buffer());
    }

    public Location write(JournalByteBuffer buffer) {
        ByteBuffer writableBuffer = buffer.getWritableBuffer();
        int remaining = writableBuffer.remaining();
        long bufferPosition = virtualPosition.getAndAdd(remaining);
        writingQueue.put(bufferPosition, writableBuffer);
        return new Location(bufferPosition);
    }
}
