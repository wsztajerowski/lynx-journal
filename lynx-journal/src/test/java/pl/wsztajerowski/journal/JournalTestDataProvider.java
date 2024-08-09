package pl.wsztajerowski.journal;

import pl.wsztajerowski.journal.records.RecordTestDataProvider;
import pl.wsztajerowski.journal.records.V1SchemaRecordTestDataProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static pl.wsztajerowski.journal.BytesTestUtils.intToBytes;
import static pl.wsztajerowski.journal.FilesTestUtils.appendToFile;

public class JournalTestDataProvider {
    private final Path journalFilePath;
    private final RecordTestDataProvider recordTestDataProvider;

    public JournalTestDataProvider(Path journalFilePath, RecordTestDataProvider recordTestDataProvider) {
        this.journalFilePath = journalFilePath;
        this.recordTestDataProvider = recordTestDataProvider;
    }

    public static JournalTestDataProvider validJournal() throws IOException {
        return validJournal(Files.createTempFile("journal-test", ".dat"));
    }

    public static JournalTestDataProvider validJournal(Path filePath) throws IOException {
        appendToFile(filePath, intToBytes(Journal.JOURNAL_PREFIX));
        appendToFile(filePath, intToBytes(Journal.SCHEMA_VERSION_V1));
        RecordTestDataProvider recordTestDataProvider = V1SchemaRecordTestDataProvider.recordTestDataProvider(filePath);
        return new JournalTestDataProvider(filePath, recordTestDataProvider);
    }

    public static JournalTestDataProvider journalWithInvalidPrefix() throws IOException {
        return journalWithInvalidPrefix(Files.createTempFile("journal-test", ".dat"));
    }

    public static JournalTestDataProvider journalWithInvalidPrefix(Path filePath) throws IOException {
        appendToFile(filePath, intToBytes(0xDEADC0DE));
        appendToFile(filePath, intToBytes(Journal.SCHEMA_VERSION_V1));
        RecordTestDataProvider recordTestDataProvider = V1SchemaRecordTestDataProvider.recordTestDataProvider(filePath);
        return new JournalTestDataProvider(filePath, recordTestDataProvider);
    }

    public static JournalTestDataProvider journalWithUnsupportedSchemaVersion() throws IOException {
        return journalWithUnsupportedSchemaVersion(Files.createTempFile("journal-test", ".dat"));
    }

    public static JournalTestDataProvider journalWithUnsupportedSchemaVersion(Path filePath) throws IOException {
        appendToFile(filePath, intToBytes(Journal.JOURNAL_PREFIX));
        appendToFile(filePath, intToBytes(0x0FF1CE00));
        RecordTestDataProvider recordTestDataProvider = V1SchemaRecordTestDataProvider.recordTestDataProvider(filePath);
        return new JournalTestDataProvider(filePath, recordTestDataProvider);
    }

    public Path journalFilePath() {
        return journalFilePath;
    }

    public RecordTestDataProvider recordTestDataProvider() {
       return recordTestDataProvider;
    }
}
