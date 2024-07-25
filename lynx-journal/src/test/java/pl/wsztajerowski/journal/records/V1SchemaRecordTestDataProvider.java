package pl.wsztajerowski.journal.records;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static pl.wsztajerowski.journal.BytesTestUtils.*;
import static pl.wsztajerowski.journal.FilesTestUtils.appendToFile;

public class V1SchemaRecordTestDataProvider implements RecordTestDataProvider {
    private final Path journalFilePath;

    public V1SchemaRecordTestDataProvider(Path journalFilePath) {
        this.journalFilePath = journalFilePath;
    }

    public static RecordTestDataProvider recordTestDataProvider(Path journalFilePath) {
        return new V1SchemaRecordTestDataProvider(journalFilePath);
    }

    @Override
    public long saveVariable(String variable) throws IOException {
        long offset = Files.size(journalFilePath);
        appendToFile(journalFilePath, RecordHeader.RECORD_PREFIX);
        byte[] bytes = variable.getBytes(StandardCharsets.UTF_8);
        appendToFile(journalFilePath, toByteArray(bytes.length));
        appendToFile(journalFilePath, bytes);
        return offset;
    }

    @Override
    public long saveVariable(int variable) throws IOException {
        long offset = Files.size(journalFilePath);
        appendToFile(journalFilePath, RecordHeader.RECORD_PREFIX);
        appendToFile(journalFilePath, toByteArray(Integer.BYTES));
        appendToFile(journalFilePath, variable);
        return offset;
    }

    @Override
    public long saveVariableWithInvalidRecordHeader(String variable) throws IOException {
        long offset = Files.size(journalFilePath);
        appendToFile(journalFilePath, 0xDEADC0DE);
        appendToFile(journalFilePath, toByteArray(Integer.BYTES));
        appendToFile(journalFilePath, variable);
        return offset;
    }
}
