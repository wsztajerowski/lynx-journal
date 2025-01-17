package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalException;
import pl.wsztajerowski.journal.JournalRuntimeIOException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecordWriteChannel implements AutoCloseable, Runnable {
    private static final long WRITE_CHUNK_SIZE = 524_288L;
    private final ConcurrentNavigableMap<Long, ByteBuffer> queue;
    private final FileChannel fileChannel;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    RecordWriteChannel(FileChannel fileChannel, ConcurrentNavigableMap<Long, ByteBuffer> queue) {
        this.fileChannel = fileChannel;
        this.queue = queue;
    }

    public static RecordWriteChannel open(Path journalFile, ConcurrentNavigableMap<Long, ByteBuffer> queue) {
        try {
            FileChannel writerChannel = FileChannel.open(journalFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            writerChannel.position(writerChannel.size());
            return new RecordWriteChannel(writerChannel, queue);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void close() throws IOException {
        isClosed.set(true);
        fileChannel.close();
    }

    public void run() {
        try {
            long lastWrittenPosition = fileChannel.position();
            while (!Thread.currentThread().isInterrupted() && !isClosed.get()) {
                if (queue.isEmpty()) {
                    Thread.onSpinWait();
                    continue;
                }
                Long lastPositionToWrite = queue.lowerKey(lastWrittenPosition + WRITE_CHUNK_SIZE);
//                Long lastPosition = queue.lastKey();
                var recordsToWrite = queue.headMap(lastPositionToWrite, true);
//            System.out.println("Try writing " + recordsToWrite.size() + " records from queue");
//            System.out.println("Virtual locations: " + recordsToWrite.keySet().stream().toList());
                if (recordsToWrite.isEmpty()) {
                    Thread.onSpinWait();
                }
                try {
                    ByteBuffer[] buffers = recordsToWrite.values().toArray(new ByteBuffer[recordsToWrite.size()]);
                    int totalBytesToWrite = 0;
//                int emptyBuffers = 0;
                    for (ByteBuffer buffer : buffers) {
                        totalBytesToWrite += buffer.remaining();
//                    if (!buffer.hasRemaining()) {
//                        emptyBuffers++;
//                    }
                    }
//                System.out.println("Empty buffers: " + emptyBuffers);
//                System.out.println("Expecting write " + expectedBytes + " bytes to " + recordsToWrite.firstKey());
//                System.out.println("File position before write: " + fileChannel.position());
//                long writtenBytes = fileChannel.write(buffers);
//                System.out.println("File position after write: " + fileChannel.position());
                    long totalBytesWritten = 0;
                    while (totalBytesWritten < totalBytesToWrite) {
                        totalBytesWritten += fileChannel.write(buffers);
                    }

                    if (totalBytesWritten != totalBytesToWrite) {
                        throw new JournalRuntimeIOException("Written bytes mismatch - expected: " + totalBytesToWrite + ", actual: " + totalBytesWritten);
                    }
//                System.out.println("Wrote " + writtenBytes + " bytes to " + lastPosition);
                    recordsToWrite.keySet().removeIf( key -> true );
//                System.out.println("Number of waiting writes: " + queue.size());
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                    throw new JournalException("Error writing to FileChannel", e);
                }
            }
        } catch (IOException e) {
            throw new JournalRuntimeIOException(e);
        }
    }

    public long getCurrentPosition() {
        try {
            return fileChannel.position();
        } catch (IOException e) {
            throw new JournalRuntimeIOException(e);
        }
    }

}
