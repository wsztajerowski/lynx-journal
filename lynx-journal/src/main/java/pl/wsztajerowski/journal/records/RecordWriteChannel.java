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

import static pl.wsztajerowski.journal.records.ChecksumCalculator.computeChecksum;
import static pl.wsztajerowski.journal.records.RecordHeader.RECORD_PREFIX;
import static pl.wsztajerowski.journal.records.RecordHeader.recordHeaderLength;

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
        var buffer = journalBuffer.getContentBuffer();
        int variableSize = buffer.remaining();
        if (variableSize == 0) {
            throw new JournalRuntimeIOException("Buffer contains no data to write");
        }
        var checksum = computeChecksum(buffer);
        prepareRecordHeaderBufferToWrite(variableSize, checksum, journalBuffer.getHeaderBuffer());
        lock.lock();
        try {
            var location = new Location(fileChannel.position());
            int writtenRecordBytes = fileChannel.write(journalBuffer.getWritableBuffer());
            int expectedRecordSize = recordHeaderLength() + variableSize;
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

    private void prepareRecordHeaderBufferToWrite(int variableSize, int checksum, ByteBuffer headerBuffer) {
        headerBuffer.putInt(0, RECORD_PREFIX);
        headerBuffer.putInt(4, variableSize);
        headerBuffer.putInt(8, checksum);
        headerBuffer.rewind();
    }
}
