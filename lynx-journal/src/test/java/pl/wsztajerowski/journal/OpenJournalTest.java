package pl.wsztajerowski.journal;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import pl.wsztajerowski.journal.exceptions.InvalidJournalHeader;
import pl.wsztajerowski.journal.exceptions.TooSmallJournalHeader;
import pl.wsztajerowski.journal.exceptions.UnsupportedJournalVersion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.JournalTestDataProvider.journalWithInvalidPrefix;
import static pl.wsztajerowski.journal.JournalTestUtils.journalCurrentSchemaVersionInHexString;
import static pl.wsztajerowski.journal.JournalTestUtils.journalHeaderPrefixInHexString;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenJournalTest {
    private Journal sut;

    @AfterEach
    void tearDown() throws IOException {
        if (sut != null){
            sut.closeJournal();
        }
    }

    @Test
    void opened_journal_has_only_valid_header() throws IOException {
        // given
        Path journalPath = createTempFile("journal", ".dat");

        // when
        sut = Journal.open(journalPath, false);

        // then
        byte[] bytes = readAllBytes(journalPath);
        assertThat(bytes)
            .hasSize(8)
            .asHexString()
            .containsSequence(journalHeaderPrefixInHexString(), journalCurrentSchemaVersionInHexString());
    }

    @Test
    void open_journal_with_unsupported_schema_version_throws_exception() throws IOException {
        // given
        Path journalFilePath = JournalTestDataProvider.journalWithUnsupportedSchemaVersion()
            .journalFilePath();

        // when
        Exception exception = Assertions.catchException(() -> Journal.open(journalFilePath, false));

        // then
        assertThat(exception)
            .isInstanceOf(UnsupportedJournalVersion.class);
    }

    @Test
    void open_journal_with_invalid_prefix_fails() throws IOException {
        // given
        Path journalFilePath = journalWithInvalidPrefix()
            .journalFilePath();

        // when
        Exception exception = Assertions.catchException(() -> Journal.open(journalFilePath, false));

        // then
        assertThat(exception)
            .isInstanceOf(InvalidJournalHeader.class)
            .hasMessageContaining("Invalid journal header prefix");
    }

    @Test
    void open_journal_with_without_full_header_fails() throws IOException {
        // given
        Path journalPath = createTempFile("journal", ".dat");
        Files.write(journalPath, BytesTestUtils.toByteArray(1));

        // when
        Exception exception = Assertions.catchException(() -> Journal.open(journalPath, false));

        // then
        assertThat(exception)
            .isInstanceOf(TooSmallJournalHeader.class);
    }

}