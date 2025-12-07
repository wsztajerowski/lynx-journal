package pl.wsztajerowski.journal;

import org.junit.jupiter.api.*;
import pl.wsztajerowski.journal.records.ChecksumCalculator;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseHexString;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseUtf8HexString;
import static pl.wsztajerowski.journal.FilesTestUtils.*;
import static pl.wsztajerowski.journal.records.JournalByteBufferFactory.createJournalByteBuffer;
import static pl.wsztajerowski.journal.records.RecordTestUtils.recordHeaderPrefixInHexString;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ReadYourOwnWritesTest {
    public static final int BATCH_SIZE = 64;
    private Journal sut;
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("journal", ".dat");
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.close();
    }

    @Test
    void write_single_buffer_with_size_of_batch_and_read_it_back() {
        // given
        var content = "Hello World";
        var buffer = FilesTestUtils.wrapInJournalByteBufferWithSize(content, BATCH_SIZE);

        // when
        var location = sut.write(buffer);
        // and
        var readContentBuffer = sut.read(createJournalByteBuffer(64), location);

        // then
        var readContent = readAsUtf8(readContentBuffer);
        assertThat(readContent)
            .startsWith(content);
    }

    @Test
    void journal_file_has_all_corrent_form_after_close() throws IOException {
        // given
        var content = "Hello World";
        var buffer = FilesTestUtils.wrapInJournalByteBuffer(content);

        // when
        sut.writeAsync(buffer);
        // and
        sut.close();

        // then
        assertThat(readAllBytes(dataFilePath))
            .asHexString()
            .containsSequence(
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(content.length()),
                toUpperCaseHexString(ChecksumCalculator.computeChecksum(content)),
                toUpperCaseUtf8HexString(content));
    }

    @Test
    void write_buffer_with_size_of_batch_flushes_previous_writes() {
        // given
        var firstVariableContent = "My";
        var secondVariableContent = "fantastic";
        var thirdVariableContent = "project!";

        // when
        var firstVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(firstVariableContent));
        var secondVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(secondVariableContent));
        var thirdVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(thirdVariableContent));
        sut.writeAsync(wrapInJournalByteBufferWithSize("STOP", BATCH_SIZE));

        // and
        var secondReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), secondVariableLocation));
        var firstReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), firstVariableLocation));
        var thirdReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), thirdVariableLocation));

        // then
        assertThat(String.join(" ", firstReadContent, secondReadContent, thirdReadContent))
            .isEqualTo("My fantastic project!");
    }

    @Test
    void write_3_buffers_with_total_size_smaller_than_batch_size_persist_data_after_journal_close() throws IOException {
        // given
        var firstVariableContent = "My";
        var secondVariableContent = "fantastic";
        var thirdVariableContent = "project!";

        // when
        var firstVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(firstVariableContent));
        var secondVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(secondVariableContent));
        var thirdVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(thirdVariableContent));

        sut.close();
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);
        // and
        var secondReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), secondVariableLocation));
        var firstReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), firstVariableLocation));
        var thirdReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), thirdVariableLocation));

        // then
        assertThat(String.join(" ", firstReadContent, secondReadContent, thirdReadContent))
            .isEqualTo("My fantastic project!");
    }

    @Test
    @DisplayName("3 writes in a row: write(0.5xBATCH_SIZE), write(0.5xBATCH_SIZE), write(0,5xBATCH_SIZE) flushes data after second write, and after close()")
    void few_writes_smaller_than_batch_size() throws IOException {
        // given
        var variableContent1 = "WRITE_1";
        var variableContent2 = "WRITE_2";
        var variableContent3 = "WRITE_3";

        // when
        var location1 = sut.writeAsync(wrapInJournalByteBufferWithSize(variableContent1, BATCH_SIZE / 2));
        var location2 = sut.write(wrapInJournalByteBufferWithSize(variableContent2, BATCH_SIZE / 2));

        // then
        var readContent1 = readAsUtf8(sut.read(createJournalByteBuffer(64), location1));
        assertThat(readContent1)
            .startsWith(variableContent1);
        var readContent2 = readAsUtf8(sut.read(createJournalByteBuffer(64), location2));
        assertThat(readContent2)
            .startsWith(variableContent2);

        // and when
        var location3 = sut.writeAsync(wrapInJournalByteBufferWithSize(variableContent3, BATCH_SIZE / 2));
        sut.close();
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);

        // then
        var readContent3 = readAsUtf8(sut.read(createJournalByteBuffer(64), location3));
        assertThat(readContent3)
            .startsWith(variableContent3);

        // and
        assertThat(String.join(" ",
            readAsUtf8(sut.read(createJournalByteBuffer(64), location2)),
            readAsUtf8(sut.read(createJournalByteBuffer(64), location1)),
            readAsUtf8(sut.read(createJournalByteBuffer(64), location3))
        )).containsSubsequence(variableContent2, variableContent1, variableContent3);
    }

    @Test
    void write_single_buffer_smaller_than_batch_size_and_read_it_back() throws IOException {
        // given
        var content = "Hello World";
        var buffer = FilesTestUtils.wrapInJournalByteBuffer(content);

        // when
        var location = sut.writeAsync(buffer);

        // and
        sut.close();
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);

        // then
        var readContentBuffer = sut.read(createJournalByteBuffer(64), location);
        var readContent = readAsUtf8(readContentBuffer);
        assertThat(readContent)
            .isEqualTo(content);
    }
}
