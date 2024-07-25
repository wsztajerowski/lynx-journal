package pl.wsztajerowski.journal;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import pl.wsztajerowski.journal.exceptions.InvalidJournalHeader;
import pl.wsztajerowski.journal.exceptions.UnsupportedJournalVersion;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.JournalTestDataProvider.journalWithInvalidPrefix;
import static pl.wsztajerowski.journal.JournalTestUtils.journalCurrentSchemaVersionInHexString;
import static pl.wsztajerowski.journal.JournalTestUtils.journalHeaderPrefixInHexString;

class OpenJournalTest {
    private Journal sut;

    @AfterEach
    void tearDown() throws IOException {
        if (sut != null){
            sut.closeJournal();
        }
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
            .containsSequence(journalHeaderPrefixInHexString(), journalCurrentSchemaVersionInHexString());
    }

    @Test
    void openJournalWithUnsupportedSchemaVersionThrowsException() throws IOException {
        // given
        Path journalFilePath = JournalTestDataProvider.journalWithUnsupportedSchemaVersion()
            .journalFilePath();

        // when
        Exception exception = Assertions.catchException(() -> Journal.open(journalFilePath));

        // then
        assertThat(exception)
            .isInstanceOf(UnsupportedJournalVersion.class);
    }

    @Test
    void openCorruptedJournalFails() throws IOException {
        // given
        Path journalFilePath = journalWithInvalidPrefix()
            .journalFilePath();

        // when
        Exception exception = Assertions.catchException(() -> Journal.open(journalFilePath));

        // then
        assertThat(exception)
            .isInstanceOf(InvalidJournalHeader.class)
            .hasMessageContaining("Invalid journal header format");
    }

}