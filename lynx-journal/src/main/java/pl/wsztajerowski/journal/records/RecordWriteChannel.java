package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.ByteBuffer.allocate;
import static pl.wsztajerowski.journal.records.RecordHeader.RECORD_PREFIX;
import static pl.wsztajerowski.journal.records.RecordHeader.recordHeaderLength;

public class RecordWriteChannel {
    private final ByteBuffer recordHeaderBuffer;
    private final FileChannel fileChannel;

    RecordWriteChannel(ByteBuffer recordHeaderBuffer, FileChannel fileChannel) {
        this.recordHeaderBuffer = recordHeaderBuffer;
        this.fileChannel = fileChannel;
    }

    public static RecordWriteChannel open(FileChannel fileChannel) {
        var headerBuffer = allocate(recordHeaderLength())
            .putInt(RECORD_PREFIX);
        return new RecordWriteChannel(headerBuffer, fileChannel);
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    public Location append(ByteBuffer buffer) throws IOException {
        // [0, 128, 128] => 128B [128,128,128]
        // [40, 80, 128] => 40B [80,80,128]
        var location = new Location(fileChannel.position());
        prepareRecordHeaderBufferToWrite(buffer.remaining());
        fileChannel.write(recordHeaderBuffer);
        fileChannel.write(buffer);
        return location;
    }

    private void prepareRecordHeaderBufferToWrite(int variableSize) {
        recordHeaderBuffer.putInt(4, variableSize);
        recordHeaderBuffer.rewind();
    }
}
