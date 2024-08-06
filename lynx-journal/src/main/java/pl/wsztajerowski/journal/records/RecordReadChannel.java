package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.exceptions.InvalidRecordHeader;
import pl.wsztajerowski.journal.exceptions.JournalRuntimeIOException;
import pl.wsztajerowski.journal.exceptions.NotEnoughSpaceInBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
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
        var variableBuffer = readFromFileChannel(localCopyOfDestination, location.offset() + recordHeaderLength());
        return new Record(recordHeader, location, variableBuffer);
    }

    private RecordHeader validateAndGetRecordHeader(Location location) {
        // v01 record header format: [ int prefix, int variableSize ]
        recordHeaderBuffer.clear();
        var headerBuffer = readFromFileChannel(recordHeaderBuffer, location.offset());
        int prefix = headerBuffer.getInt();
        int variableSize = headerBuffer.getInt();
        if (prefix != RecordHeader.RECORD_PREFIX) {
            throw new InvalidRecordHeader(prefix, variableSize);
        }
        return new RecordHeader(variableSize);
    }

    private ByteBuffer readFromFileChannel(ByteBuffer buffer, long offset) {
        try {
            fileChannel.read(buffer, offset);
        } catch (IOException e) {
            throw new JournalRuntimeIOException("Error during reading from fileChannel", e);
        }
        buffer.rewind();
        return buffer;
    }
}
