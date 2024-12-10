package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalRuntimeIOException;
import pl.wsztajerowski.journal.Location;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

public class RecordWriteChannel implements AutoCloseable {
    private final FileChannel fileChannel;
    private final ReentrantLock lock;

    RecordWriteChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.lock = new ReentrantLock();
    }

    public static RecordWriteChannel open(Path journalFile) {
        try {
            FileChannel writerChannel = FileChannel.open(journalFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            writerChannel.position(writerChannel.size());
            return new RecordWriteChannel(writerChannel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void close() throws IOException {
            fileChannel.close();
    }

    public Location append(JournalByteBuffer journalBuffer) {
        ByteBuffer writableBuffer = journalBuffer.getWritableBuffer();
        var expectedRecordSize = writableBuffer.limit();
        lock.lock();
        try {
            var location = new Location(fileChannel.position());
            var writtenRecordBytes = fileChannel.write(writableBuffer);
            if (writtenRecordBytes != expectedRecordSize) {
                throw new JournalRuntimeIOException("Number of bytes written to channel (%d) is different than sum of record's header size and input variable size (%d)".formatted(writtenRecordBytes, expectedRecordSize));
            }
            return location;
        } catch (IOException e) {
            throw new JournalRuntimeIOException(e);
        } finally {
            lock.unlock();
        }
    }
}
