package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.JournalRuntimeIOException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.nio.ByteBuffer.allocate;
import static pl.wsztajerowski.journal.records.ChecksumCalculator.computeChecksum;
import static pl.wsztajerowski.journal.records.RecordHeader.RECORD_PREFIX;
import static pl.wsztajerowski.journal.records.RecordHeader.recordHeaderLength;

public class RecordWriteChannel implements AutoCloseable {
    private final ByteBuffer recordHeaderBuffer;
    private final FileChannel fileChannel;

    RecordWriteChannel(ByteBuffer recordHeaderBuffer, FileChannel fileChannel) {
        this.recordHeaderBuffer = recordHeaderBuffer;
        this.fileChannel = fileChannel;
    }

    public static RecordWriteChannel open(Path journalFile) {
        try {
            var headerBuffer = allocate(recordHeaderLength())
                .putInt(RECORD_PREFIX);
            FileChannel writerChannel = FileChannel.open(journalFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            writerChannel.position(writerChannel.size());
            return new RecordWriteChannel(headerBuffer, writerChannel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void close() throws IOException {
            fileChannel.close();
    }

    public Location append(ByteBuffer buffer) {
        try {
            var location = new Location(fileChannel.position());
            int variableSize = buffer.remaining();
            if (variableSize == 0) {
                throw new JournalRuntimeIOException("Buffer contains no data to write");
            }
            var checksum = computeChecksum(buffer);
            prepareRecordHeaderBufferToWrite(variableSize, checksum);
            int writtenHeaderBytes = fileChannel.write(recordHeaderBuffer);
            if(writtenHeaderBytes != recordHeaderLength()){
                throw new JournalRuntimeIOException("Number of bytes written to channel (%d) is different than size of record's header (%d)".formatted(writtenHeaderBytes, recordHeaderLength()));
            }
            int writtenRecordBytes = fileChannel.write(buffer);
            if ( writtenRecordBytes != variableSize){
                throw new JournalRuntimeIOException("Number of bytes written to channel (%d) is different than size of input variable (%d)".formatted(writtenRecordBytes, variableSize));
            }
            return location;
        } catch (IOException e) {
            throw new JournalRuntimeIOException(e);
        }
    }

    private void prepareRecordHeaderBufferToWrite(int variableSize, int checksum) {
        recordHeaderBuffer.putInt(4, variableSize);
        recordHeaderBuffer.putInt(8, checksum);
        recordHeaderBuffer.rewind();
    }
}
