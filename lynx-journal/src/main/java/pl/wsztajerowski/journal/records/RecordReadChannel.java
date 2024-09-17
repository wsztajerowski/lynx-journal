package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.JournalRuntimeIOException;
import pl.wsztajerowski.journal.Location;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;
import static pl.wsztajerowski.journal.records.InvalidRecordHeaderException.invalidRecordHeaderPrefix;
import static pl.wsztajerowski.journal.records.Record.createAndValidateRecord;
import static pl.wsztajerowski.journal.records.RecordHeader.*;

public class RecordReadChannel implements AutoCloseable {
    private final ByteBuffer recordHeaderBuffer;
    private final FileChannel fileChannel;

    RecordReadChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        recordHeaderBuffer = ByteBuffer.allocate(recordHeaderLength());
    }

    public static RecordReadChannel open(Path journalPath) {
        try {
            FileChannel readerChannel = FileChannel.open(journalPath, READ);
            return new RecordReadChannel(readerChannel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void close() throws IOException {
            fileChannel.close();
    }

    public Record read(ByteBuffer destination, Location location) {
        var recordHeader = readRecordHeader(location);
        validateDestinationBufferSpace(destination, recordHeader);
        var variableBuffer = readFromFileChannel(destination, location.offset() + recordHeaderLength(), recordHeader.variableSize());
        return createAndValidateRecord(recordHeader, location, variableBuffer);
    }

    private static void validateDestinationBufferSpace(ByteBuffer destination, RecordHeader recordHeader) {
        if (destination.remaining() < recordHeader.variableSize()) {
            throw new NotEnoughSpaceInBufferException(destination.remaining(), recordHeader.variableSize());
        }
    }

    private RecordHeader readRecordHeader(Location location) {
        recordHeaderBuffer.clear();
        var headerBuffer = readFromFileChannel(recordHeaderBuffer, location.offset(), recordHeaderLength());
        int prefix = headerBuffer.getInt();
        if (prefix != RECORD_PREFIX) {
            throw invalidRecordHeaderPrefix(prefix);
        }
        return createAndValidateHeader(headerBuffer.getInt(), headerBuffer.getInt());
    }

    private ByteBuffer readFromFileChannel(ByteBuffer buffer, long offset, int expectedSize) {
        try {
            var bufferCopy = buffer
                .duplicate()
                .limit(buffer.position() + expectedSize);
            var readBytes = fileChannel.read(bufferCopy, offset);
            if (readBytes == -1) {
                throw new JournalRuntimeIOException("Read from outside of channel", new EOFException());
            } else if (readBytes != expectedSize) {
                throw new JournalRuntimeIOException("Number of read bytes from channel (%d) is different than expected (%d)".formatted(readBytes, expectedSize));
            }
            return buffer.limit(bufferCopy.limit());
        } catch (IOException e) {
            throw new JournalRuntimeIOException("Error during reading from fileChannel", e);
        }
    }
}
