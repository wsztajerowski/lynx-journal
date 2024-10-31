package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.JournalRuntimeIOException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.ByteBuffer.allocate;
import static pl.wsztajerowski.journal.records.ChecksumCalculator.computeChecksum;
import static pl.wsztajerowski.journal.records.RecordHeader.RECORD_PREFIX;
import static pl.wsztajerowski.journal.records.RecordHeader.recordHeaderLength;

public class RecordWriteChannel implements AutoCloseable {
    private final ThreadLocal<ByteBuffer> recordHeaderBuffer;
    private final FileChannel fileChannel;
    private final ReentrantLock lock;

    RecordWriteChannel(FileChannel fileChannel) {
        this.recordHeaderBuffer = ThreadLocal.withInitial(() -> allocate(recordHeaderLength())
            .putInt(RECORD_PREFIX));
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

    public Location append(ByteBuffer buffer) {
        int variableSize = buffer.remaining();
        if (variableSize == 0) {
            throw new JournalRuntimeIOException("Buffer contains no data to write");
        }
        var checksum = computeChecksum(buffer);
        prepareRecordHeaderBufferToWrite(variableSize, checksum);
        lock.lock();
        try {
            var location = new Location(fileChannel.position());
            int writtenHeaderBytes = fileChannel.write(recordHeaderBuffer.get());
            if (writtenHeaderBytes != recordHeaderLength()) {
                throw new JournalRuntimeIOException("Number of bytes written to channel (%d) is different than size of record's header (%d)".formatted(writtenHeaderBytes, recordHeaderLength()));
            }
            int writtenRecordBytes = fileChannel.write(buffer);
            if (writtenRecordBytes != variableSize) {
                throw new JournalRuntimeIOException("Number of bytes written to channel (%d) is different than size of input variable (%d)".formatted(writtenRecordBytes, variableSize));
            }
            return location;
        } catch (IOException e) {
            throw new JournalRuntimeIOException(e);
        } finally {
            lock.unlock();
        }
    }

    private void prepareRecordHeaderBufferToWrite(int variableSize, int checksum) {
        ByteBuffer localByteBuffer = recordHeaderBuffer.get();
        localByteBuffer.putInt(4, variableSize);
        localByteBuffer.putInt(8, checksum);
        localByteBuffer.rewind();
    }
}
