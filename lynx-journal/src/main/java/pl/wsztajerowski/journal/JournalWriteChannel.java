package pl.wsztajerowski.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class JournalWriteChannel {
    private final FileChannel fileChannel;

    JournalWriteChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public static JournalWriteChannel open(Path path) throws IOException {
        var fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        return new JournalWriteChannel(fileChannel);
    }

    public void writeJournalHeader() throws IOException {
        var buffer = JournalHeader.createPopulatedHeader();
        fileChannel.write(buffer);
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    public Location append(ByteBuffer buffer) throws IOException {
        var recordHeader = new RecordHeader(buffer.capacity());
        var recordPosition = fileChannel.position();
        fileChannel.write(recordHeader.getRecordHeaderBuffer());
        fileChannel.write(buffer);
        return new Location(recordPosition);
    }
}
