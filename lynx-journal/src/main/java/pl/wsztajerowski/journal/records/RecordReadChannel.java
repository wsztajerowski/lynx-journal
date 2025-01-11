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
    private static final int PAGE_SIZE = 4096;
    private final ThreadLocal<ByteBuffer> threadLocalBuffer;
    private final FileChannel fileChannel;

    RecordReadChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        threadLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(PAGE_SIZE));
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

    public Record read(JournalByteBuffer destination, Location location) {
        ByteBuffer localByteBuffer = threadLocalBuffer.get();
        localByteBuffer.clear();
        int readBytes = readPage(localByteBuffer, location.offset());
        var recordHeader = readRecordHeader(localByteBuffer, readBytes);
        ByteBuffer targetContentBuffer = destination.getContentBuffer();
        validateDestinationBufferSpaceAndSetLimit(targetContentBuffer, recordHeader);

        long offset = location.offset();
        int variableBytesToRead = recordHeader.variableSize();
        readBytes -= recordHeaderLength();
        targetContentBuffer.mark();
        do {
            boolean readIncompletePageWithRecordHeader = offset == location.offset() && (readBytes != PAGE_SIZE - recordHeaderLength());
            boolean readIncompletePageWithDataOnly = offset != location.offset() && readBytes != PAGE_SIZE;
            if ( (readIncompletePageWithRecordHeader || readIncompletePageWithDataOnly) && (readBytes < variableBytesToRead) ) {
                throw new JournalRuntimeIOException("Corrupted journal file - cannot read " + (variableBytesToRead - readBytes) + " bytes");
            }
            int numberOfBytesToCopy = Math.min(readBytes, variableBytesToRead);
            targetContentBuffer.put(targetContentBuffer.position(), localByteBuffer, localByteBuffer.position(), numberOfBytesToCopy);
            targetContentBuffer.position(targetContentBuffer.position() + numberOfBytesToCopy);
            localByteBuffer.clear();
            variableBytesToRead -= readBytes;
            offset += PAGE_SIZE;
        } while (variableBytesToRead > 0 && (readBytes = readPage(localByteBuffer, offset)) > 0);
        if (variableBytesToRead > 0) {
            throw new JournalRuntimeIOException("Corrupted journal file - cannot read " + variableBytesToRead + " bytes");
        }
        targetContentBuffer.reset();
        return createAndValidateRecord(recordHeader, location, targetContentBuffer);
    }

    private int readPage(ByteBuffer targetBuffer, long offset) {
        try {
            int readBytes = fileChannel.read(targetBuffer, offset);
            targetBuffer.flip();
            return readBytes;
        } catch (IOException e) {
            throw new JournalRuntimeIOException("Error during reading from fileChannel", e);
        }
    }

    private static void validateDestinationBufferSpaceAndSetLimit(ByteBuffer destination, RecordHeader recordHeader) {
        if (destination.remaining() < recordHeader.variableSize()) {
            throw new NotEnoughSpaceInBufferException(destination.remaining(), recordHeader.variableSize());
        }
        destination.limit(destination.position() + recordHeader.variableSize());
    }

    private RecordHeader readRecordHeader(ByteBuffer headerBuffer, int readBytes) {
        if (readBytes < recordHeaderLength()) {
            throw new JournalRuntimeIOException("Corrupted journal file - number of read bytes is smaller than record header size", new EOFException("Read from outside of channel"));
        }
        int prefix = headerBuffer.getInt();
        if (prefix != RECORD_PREFIX) {
            throw invalidRecordHeaderPrefix(prefix);
        }
        return createAndValidateHeader(headerBuffer.getInt(), headerBuffer.getInt());
    }

}
