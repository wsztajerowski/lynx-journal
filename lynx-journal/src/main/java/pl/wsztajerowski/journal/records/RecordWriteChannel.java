package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalException;
import pl.wsztajerowski.journal.JournalRuntimeIOException;
import pl.wsztajerowski.journal.Location;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecordWriteChannel implements AutoCloseable, Runnable {
    private final BlockingQueue<RecordWriteTask> queue;
    private final FileChannel fileChannel;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    RecordWriteChannel(FileChannel fileChannel, BlockingQueue<RecordWriteTask> queue) {
        this.fileChannel = fileChannel;
        this.queue = queue;
    }

    public static RecordWriteChannel open(Path journalFile, BlockingQueue<RecordWriteTask> queue) {
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
        while (!Thread.currentThread().isInterrupted() && !isClosed.get()) {
            List<RecordWriteTask> tasks = new ArrayList<>();
            queue.drainTo(tasks);
            if (tasks.isEmpty()) {
                continue;
            }
            try {
                dumpRecordsToFile(tasks);
            } catch (Exception e) {
                for (RecordWriteTask task : tasks) {
                    task.completeExceptionally(e);
                }
                Thread.currentThread().interrupt();
                throw new JournalException("Error writing to FileChannel", e);
            }
        }
    }

    void dumpRecordsToFile(List<RecordWriteTask> tasks) throws IOException {
        List<ByteBuffer> buffers = new ArrayList<>();
        long expectedNumberOfWrittenBytes = 0;
        for (RecordWriteTask task : tasks) {
            ByteBuffer writableBuffer = task.byteBuffer();
            expectedNumberOfWrittenBytes += writableBuffer.limit();
            buffers.add(writableBuffer);
        }
        long position = fileChannel.position();
        long writtenBytes = fileChannel.write(buffers.toArray(new ByteBuffer[0]));
        if (writtenBytes != expectedNumberOfWrittenBytes) {
            throw new JournalRuntimeIOException("Number of bytes written to channel (%d) is different than sum of record's header size and input variable size (%d)"
                .formatted(writtenBytes, expectedNumberOfWrittenBytes));
        }
        for (RecordWriteTask task : tasks) {
            task.future().complete(new Location(position));
            position += task.byteBuffer().limit();
        }
    }
}
