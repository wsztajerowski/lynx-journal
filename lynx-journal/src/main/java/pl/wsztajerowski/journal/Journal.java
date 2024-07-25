package pl.wsztajerowski.journal;

import pl.wsztajerowski.journal.exceptions.InvalidJournalHeader;
import pl.wsztajerowski.journal.exceptions.TooSmallJournalHeader;
import pl.wsztajerowski.journal.exceptions.UnsupportedJournalVersion;
import pl.wsztajerowski.journal.records.Record;
import pl.wsztajerowski.journal.records.RecordReadChannel;
import pl.wsztajerowski.journal.records.RecordWriteChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

public class Journal {
    private static final int NUMBER_OF_INTS_IN_HEADER = 2;
    static final int JOURNAL_PREFIX = 0xCAFEBABE;
    static final int SCHEMA_VERSION_V1 = 0x0FF1CE01;
    static final List<Integer> SUPPORTED_SCHEMA_VERSIONS = List.of(SCHEMA_VERSION_V1);
    private final RecordReadChannel readChannel;
    private final RecordWriteChannel writeChannel;

    Journal(RecordReadChannel readChannel, RecordWriteChannel writeChannel) {
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;
    }

    static int journalHeaderLength() {
        return NUMBER_OF_INTS_IN_HEADER * Integer.BYTES;
    }

    public static Journal open(Path path) throws IOException {
        // FIXME: FileChannel.open() with StandardOption.CREATE throws NoSuchFileException
        if (Files.notExists(path)){
            Files.createFile(path);
        }
        FileChannel writerChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        FileChannel readerChannel = FileChannel.open(path, CREATE, READ);

        long journalFileSize = readerChannel.size();
        if (journalFileSize > 0 && journalFileSize < journalHeaderLength()) {
            throw new TooSmallJournalHeader();
        }

        // v01 journal header format: [ int prefix, int schemaVersion ]
        ByteBuffer journalHeaderBuffer = ByteBuffer.allocate(journalHeaderLength());
        if (journalFileSize == 0) {
            journalHeaderBuffer.putInt(JOURNAL_PREFIX);
            journalHeaderBuffer.putInt(SCHEMA_VERSION_V1);
            journalHeaderBuffer.flip();
            writerChannel.write(journalHeaderBuffer);
        } else {
            readerChannel.read(journalHeaderBuffer, 0);
            journalHeaderBuffer.rewind();
            if (journalHeaderBuffer.getInt() != JOURNAL_PREFIX) {
                throw new InvalidJournalHeader();
            }
            int schemaVersion = journalHeaderBuffer.getInt();
            if (!SUPPORTED_SCHEMA_VERSIONS.contains(schemaVersion)) {
                throw new UnsupportedJournalVersion(schemaVersion);
            }
        }
        RecordWriteChannel writeChannel = RecordWriteChannel.open(writerChannel);
        RecordReadChannel readChannel = RecordReadChannel.open(readerChannel);
        return new Journal(readChannel, writeChannel);
    }

    public void closeJournal() throws IOException {
        readChannel.close();
        writeChannel.close();
    }

    public Record readRecord(ByteBuffer destination, Location location) throws IOException {
        return readChannel.read(destination, location);
    }

    public ByteBuffer read(ByteBuffer destination, Location location) throws IOException {
        return readChannel
            .read(destination, location)
            .buffer();
    }

    public Location write(ByteBuffer buffer) throws IOException {
        return writeChannel.append(buffer);
    }
}
