package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalException;
import pl.wsztajerowski.journal.JournalRuntimeIOException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecordWriteChannel implements AutoCloseable, Runnable {
    private final FileChannel fileChannel;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final DoubleBatch doubleBatch;

    RecordWriteChannel(FileChannel fileChannel, DoubleBatch doubleBatch) {
        this.fileChannel = fileChannel;
        this.doubleBatch = doubleBatch;
    }

    public static RecordWriteChannel open(Path journalFile, DoubleBatch doubleBatch) {
        try {
            FileChannel writerChannel = FileChannel.open(journalFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            writerChannel.position(writerChannel.size());
            return new RecordWriteChannel(writerChannel, doubleBatch);
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
            Batch currentBatch = doubleBatch.getCurrentBatch();
            if (currentBatch.isEmpty()) {
                Thread.onSpinWait();
                continue;
            }
            try {
                doubleBatch.switchBatch();
                ByteBuffer writableBuffer = currentBatch.writableBuffer();
                int expectedBytesToWrite = writableBuffer.remaining();
                long totalBytesWritten = fileChannel.write(writableBuffer);
//                System.out.println("Empty buffers: " + emptyBuffers);
//                System.out.println("Expecting write " + expectedBytes + " bytes to " + recordsToWrite.firstKey());
//                System.out.println("File position before write: " + fileChannel.position());
//                long writtenBytes = fileChannel.write(buffers);
//                System.out.println("File position after write: " + fileChannel.position());
                if (totalBytesWritten != expectedBytesToWrite) {
                    throw new JournalRuntimeIOException("Written bytes mismatch - expected: " + expectedBytesToWrite + ", actual: " + totalBytesWritten);
                }
                currentBatch.clear();
            } catch (Exception e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
                throw new JournalException("Error writing to FileChannel", e);
            }
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
