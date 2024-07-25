package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.exceptions.InvalidRecordHeader;
import pl.wsztajerowski.journal.exceptions.NotEnoughSpaceInBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static pl.wsztajerowski.journal.records.RecordHeader.recordHeaderLength;

public class RecordReadChannel {
    private final ByteBuffer recordHeaderBuffer;
    private final FileChannel fileChannel;

    RecordReadChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        recordHeaderBuffer = ByteBuffer.allocate(recordHeaderLength());
    }

    public static RecordReadChannel open(FileChannel fileChannel) {
        return new RecordReadChannel(fileChannel);
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    public Record read(ByteBuffer destination, Location location) throws IOException {
        var recordHeader = validateAndGetRecordHeader(location);
        if (destination.remaining() < recordHeader.variableSize()){
            throw new NotEnoughSpaceInBuffer(destination.remaining(), recordHeader.variableSize());
        }
        ByteBuffer localCopyOfDestination = destination
            .duplicate()
            .limit(destination.position() + recordHeader.variableSize());
        var variableBuffer = readFromFileChannel(location.offset() + recordHeaderLength(), localCopyOfDestination);
        return new Record(recordHeader, location, variableBuffer);
    }

    private RecordHeader validateAndGetRecordHeader(Location location) throws IOException {
        // v01 record header format: [ int prefix, int variableType, int variableSize ]
        recordHeaderBuffer.clear();
        var headerBuffer = readFromFileChannel(location.offset(), recordHeaderBuffer);
        if (headerBuffer.getInt() != RecordHeader.RECORD_PREFIX) {
            throw new InvalidRecordHeader();
        }
        return new RecordHeader(headerBuffer.getInt());
    }

    private ByteBuffer readFromFileChannel(long location, ByteBuffer buffer) throws IOException {
        fileChannel.read(buffer, location);
        buffer.rewind();
        return buffer;
    }
}
