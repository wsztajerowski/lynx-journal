package pl.wsztajerowski.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class Journal {
    private final JournalReadChannel readChannel;
    private final JournalWriteChannel writeChannel;
    private final int journalSchemaVersion;

    Journal(JournalReadChannel readChannel, JournalWriteChannel writeChannel, int journalSchemaVersion) {
        this.readChannel = readChannel;
        this.writeChannel = writeChannel;
        this.journalSchemaVersion = journalSchemaVersion;
    }

    public static Journal open(Path path) throws IOException {
        // FIXME: FileChannel.open() with StandartOption.CREATE throws NoSuchFileException
        if (Files.notExists(path)){
            Files.createFile(path);
        }
        JournalWriteChannel writeChannel = JournalWriteChannel.open(path);
        JournalReadChannel readChannel = JournalReadChannel.open(path);
        if (readChannel.isEmpty()) {
            writeChannel.writeJournalHeader();
            return new Journal(readChannel, writeChannel, JournalHeader.SCHEMA_VERSION);
        }
        int journalSchemaVersion = readChannel.validateHeaderAndGetSchemaVersion();
        return new Journal(readChannel, writeChannel, journalSchemaVersion);
    }

    public void closeJournal() throws IOException {
        readChannel.close();
        writeChannel.close();
    }

    public ByteBuffer read(Location location) throws IOException {
        Record record = readChannel
            .read(location);
        return record.buffer();
    }

    public Location write(ByteBuffer buffer) throws IOException {
        return writeChannel.append(buffer);
    }

    public int getJournalSchemaVersion() {
        return journalSchemaVersion;
    }
}
