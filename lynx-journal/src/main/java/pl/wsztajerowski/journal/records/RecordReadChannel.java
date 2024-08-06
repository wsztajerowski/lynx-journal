package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.exceptions.InvalidRecordHeader;
import pl.wsztajerowski.journal.exceptions.JournalRuntimeIOException;
import pl.wsztajerowski.journal.exceptions.NotEnoughSpaceInBuffer;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static pl.wsztajerowski.journal.records.RecordHeader.RECORD_PREFIX;
import static pl.wsztajerowski.journal.records.RecordHeader.recordHeaderLength;

public class RecordReadChannel {
    private final ByteBuffer recordHeaderBuffer;
    private final FileChannel fileChannel;

    RecordReadChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        recordHeaderBuffer = ByteBuffer.allocate(recordHeaderLength());
    }

    public static RecordReadChannel open(Path journalPath) throws IOException {
        FileChannel readerChannel = FileChannel.open(journalPath, CREATE, READ);
        return new RecordReadChannel(readerChannel);
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    public Record read(ByteBuffer destination, Location location){
        var recordHeader = validateAndGetRecordHeader(location);
        if (destination.remaining() < recordHeader.variableSize()){
            throw new NotEnoughSpaceInBuffer(destination.remaining(), recordHeader.variableSize());
        }
        ByteBuffer localCopyOfDestination = destination
            .duplicate()
            .limit(destination.position() + recordHeader.variableSize());
        var variableBuffer = readFromFileChannel(localCopyOfDestination, location.offset() + recordHeaderLength(), recordHeader.variableSize());
        return new Record(recordHeader, location, variableBuffer);
    }

    private RecordHeader validateAndGetRecordHeader(Location location) {
        // v01 record header format: [ int prefix, int variableSize ]
        recordHeaderBuffer.clear();
        var headerBuffer = readFromFileChannel(recordHeaderBuffer, location.offset(), recordHeaderLength());
        int prefix = headerBuffer.getInt();
        int variableSize = headerBuffer.getInt();
        if (prefix != RECORD_PREFIX || variableSize < 1 ) {
            throw new InvalidRecordHeader(prefix, variableSize);
        }
        return new RecordHeader(variableSize);
    }

    private ByteBuffer readFromFileChannel(ByteBuffer buffer, long offset, int expectedSize) {
        try {
            int readBytes = fileChannel.read(buffer, offset);
            if (readBytes == -1) {
                throw new JournalRuntimeIOException("Read from outside of channel", new EOFException());
            } else if (readBytes != expectedSize) {
                throw new JournalRuntimeIOException("Number of read bytes from channel (%d) is different than expected (%d)".formatted(readBytes, expectedSize));
            }
            return  buffer.flip();
        } catch (IOException e) {
            throw new JournalRuntimeIOException("Error during reading from fileChannel", e);
        }
    }
}
