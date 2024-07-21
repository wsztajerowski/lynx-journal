package pl.wsztajerowski.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static pl.wsztajerowski.journal.JournalHeader.JOURNAL_HEADER_SIZE_IN_BYTES;
import static pl.wsztajerowski.journal.JournalHeader.JOURNAL_PREFIX;

public class JournalReadChannel {
    private final FileChannel fileChannel;

    JournalReadChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public static JournalReadChannel open(Path path) throws IOException {
        var fileChannel = FileChannel.open(path, CREATE, READ);// FIXME: CREATE StandardOption seems to doesn't work on MacOS
        return new JournalReadChannel(fileChannel);
    }

    public boolean isEmpty() throws IOException {
        return fileChannel.size() == 0;
    }

    public int validateHeaderAndGetSchemaVersion() throws IOException {
        if (fileChannel.size() < JOURNAL_HEADER_SIZE_IN_BYTES){
            throw new IOException("Corrupted journal file - header size is too small");
        }
        var journalHeader = readFromFileChannel(JOURNAL_HEADER_SIZE_IN_BYTES, 0).asIntBuffer();
        if(journalHeader.get(0) != JOURNAL_PREFIX) {
            throw new IOException("Invalid journal header format");
        }
        int journalSchemaVersion = journalHeader.get(1);
        return journalSchemaVersion;
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    public Record read(Location location) throws IOException {
        var recordHeader = validateAndGetRecordHeader(location);
        var variableBuffer = readFromFileChannel(recordHeader.variableSize(), location.offset() + RecordHeader.RECORD_HEADER_SIZE_IN_BYTES);
        return new Record(recordHeader, variableBuffer.asReadOnlyBuffer());
    }

    private RecordHeader validateAndGetRecordHeader(Location location) throws IOException {
        var headerBuffer = readFromFileChannel(RecordHeader.RECORD_HEADER_SIZE_IN_BYTES, location.offset());
        var header = headerBuffer.asIntBuffer();
        if (header.get(0) != RecordHeader.RECORD_PREFIX) {
            throw new IOException("Invalid record header format");
        }
        return new RecordHeader(header.get(1), header.get(2));
    }

    private ByteBuffer readFromFileChannel(int numberOfBytesToRead, long location) throws IOException {
        var variableBuffer = ByteBuffer.allocate(numberOfBytesToRead);
        fileChannel.read(variableBuffer, location);
        variableBuffer.flip();
        return variableBuffer;
    }
}
