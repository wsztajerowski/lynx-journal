package pl.wsztajerowski.journal;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.JournalHeader.JOURNAL_HEADER_SIZE_IN_BYTES;
import static pl.wsztajerowski.journal.JournalTestUtils.*;
import static pl.wsztajerowski.journal.RecordHeader.RECORD_HEADER_SIZE_IN_BYTES;
import static pl.wsztajerowski.journal.RecordHeader.TYPE_BYTE_BUFFER;

class ReadYourOwnWritesTest {
    private Journal sut;
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("journal", ".dat");
        sut = Journal.open(dataFilePath);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.closeJournal();
    }

    @Test
    void openedJournalHasOnlyValidHeader() throws IOException {
        // given
        Path journalPath = createTempFile("journal", ".dat");

        // when
        sut = Journal.open(journalPath);

        // then
        byte[] bytes = readAllBytes(journalPath);
        assertThat(bytes)
            .hasSize(8)
            .asHexString()
            .containsSequence(journalHeaderPrefixInHexString(), journalSchemaVersionInHexString());
    }

    @Test
    void journalWithSavedVariableContainsCorrectRecord() throws IOException {
        // given
        String content = "test";
        ByteBuffer buffer = wrapInByteBuffer(content);

        // when
        sut.write(buffer);

        // then
        int expectedFileSizeInBytes = JOURNAL_HEADER_SIZE_IN_BYTES + RECORD_HEADER_SIZE_IN_BYTES + content.length();
        byte[] bytes = readAllBytes(dataFilePath);
        assertThat(bytes)
            .asHexString()
            .hasSize(expectedFileSizeInBytes * 2)
            .containsSequence(
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(TYPE_BYTE_BUFFER),
                toUpperCaseHexString(content.length()));
    }

    @Test
    void openedJournalHasCorrectSchemaVersion() {
        // when
        int schemaVersion = sut.getJournalSchemaVersion();

        // then
        assertThat(toUpperCaseHexString(schemaVersion))
            .hasSize(8)
            .startsWith("0FF1CE");
    }

    @Test
    void openCorruptedJournalFails() throws IOException {
        // given
        Path journalPath = createTempFile("journal", ".dat");
        Files.writeString(journalPath, "Some text in bytes");

        // when
        Exception exception = Assertions.catchException(() -> Journal.open(journalPath));

        // then
        assertThat(exception)
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Invalid journal header format");
    }

    @Test
    void readFromCorruptedJournalThrowsException() throws IOException {
        // given
        Files.writeString(dataFilePath, "Some text in bytes");
        var location = new Location(firstRecordOffset());

        // when
        Exception exception = Assertions.catchException(() -> sut.read(location));

        // then
        assertThat(exception)
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Invalid record header format");
    }

    @Test
    void readYourSingleWrite() throws IOException {
        // given
        var content = "Hello World";
        var buffer = wrapInByteBuffer(content);

        // when
        var location = sut.write(buffer);
        // and
        var readContentBuffer = sut.read(location);

        // then
        var readContent = readAsUtf8(readContentBuffer);
        assertThat(readContent)
            .isEqualTo(content);
    }

    @Test
    void readYourMultipleWrites() throws IOException {
        // given
        var firstVariableContent = "My";
        var secondVariableContent = " fantastic ";
        var thirdVariableContent = "project!";

        // when
        var firstVariableLocation = sut.write(wrapInByteBuffer(firstVariableContent));
        var secondVariableLocation = sut.write(wrapInByteBuffer(secondVariableContent));
        var thirdVariableLocation = sut.write(wrapInByteBuffer(thirdVariableContent));
        // and
        var secondReadContent = readAsUtf8(sut.read(secondVariableLocation));
        var firstReadContent = readAsUtf8(sut.read(firstVariableLocation));
        var thirdReadContent = readAsUtf8(sut.read(thirdVariableLocation));

        // then
        assertThat(String.join("", firstReadContent, secondReadContent, thirdReadContent))
            .isEqualTo("My fantastic project!");
    }
}
