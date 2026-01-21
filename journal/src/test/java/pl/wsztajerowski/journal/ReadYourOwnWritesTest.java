package pl.wsztajerowski.journal;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import pl.wsztajerowski.journal.records.ChecksumCalculator;
import pl.wsztajerowski.journal.records.JournalByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

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
    void setUp(@TempDir Path tempDir) throws IOException {
        dataFilePath = Files.createFile(tempDir.resolve("journal.dat"));
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.close();
    }

    @Test
    void write_single_buffer_with_size_of_batch_and_read_it_back() {
        // given
        String content = "Hello World";
        JournalByteBuffer buffer = FilesTestUtils.wrapInJournalByteBufferWithSize(content, BATCH_SIZE);

        // when
        Location location = sut.write(buffer);
        // and
        ByteBuffer readContentBuffer = sut.read(createJournalByteBuffer(64), location);

        // then
        String readContent = readAsUtf8(readContentBuffer);
        assertThat(readContent)
            .startsWith(content);
    }

    @Test
    void journal_file_has_all_corrent_form_after_close() throws IOException {
        // given
        String content = "Hello World";
        JournalByteBuffer buffer = FilesTestUtils.wrapInJournalByteBuffer(content);

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
        String firstVariableContent = "My";
        String secondVariableContent = "fantastic";
        String thirdVariableContent = "project!";

        // when
        Location firstVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(firstVariableContent));
        Location secondVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(secondVariableContent));
        Location thirdVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(thirdVariableContent));
        sut.writeAsync(wrapInJournalByteBufferWithSize("STOP", BATCH_SIZE));

        // and
        String secondReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), secondVariableLocation));
        String firstReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), firstVariableLocation));
        String thirdReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), thirdVariableLocation));

        // then
        assertThat(String.join(" ", firstReadContent, secondReadContent, thirdReadContent))
            .isEqualTo("My fantastic project!");
    }

    @Test
    void write_3_buffers_with_total_size_smaller_than_batch_size_persist_data_after_journal_close() throws IOException {
        // given
        String firstVariableContent = "My";
        String secondVariableContent = "fantastic";
        String thirdVariableContent = "project!";

        // when
        Location firstVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(firstVariableContent));
        Location secondVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(secondVariableContent));
        Location thirdVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(thirdVariableContent));

        sut.close();
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);
        // and
        String secondReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), secondVariableLocation));
        String firstReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), firstVariableLocation));
        String thirdReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), thirdVariableLocation));

        // then
        assertThat(String.join(" ", firstReadContent, secondReadContent, thirdReadContent))
            .isEqualTo("My fantastic project!");
    }

    @Test
    @DisplayName("3 writes in a row: write(0.5xBATCH_SIZE), write(0.5xBATCH_SIZE), write(0,5xBATCH_SIZE) flushes data after second write, and after close()")
    void few_writes_smaller_than_batch_size() throws IOException {
        // given
        String variableContent1 = "WRITE_1";
        String variableContent2 = "WRITE_2";
        String variableContent3 = "WRITE_3";

        // when
        Location location1 = sut.writeAsync(wrapInJournalByteBufferWithSize(variableContent1, BATCH_SIZE / 2));
        Location location2 = sut.write(wrapInJournalByteBufferWithSize(variableContent2, BATCH_SIZE / 2));

        // then
        String readContent1 = readAsUtf8(sut.read(createJournalByteBuffer(64), location1));
        assertThat(readContent1)
            .startsWith(variableContent1);
        String readContent2 = readAsUtf8(sut.read(createJournalByteBuffer(64), location2));
        assertThat(readContent2)
            .startsWith(variableContent2);

        // and when
        Location location3 = sut.writeAsync(wrapInJournalByteBufferWithSize(variableContent3, BATCH_SIZE / 2));
        sut.close();
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);

        // then
        String readContent3 = readAsUtf8(sut.read(createJournalByteBuffer(64), location3));
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
        String content = "Hello World";
        JournalByteBuffer buffer = FilesTestUtils.wrapInJournalByteBuffer(content);

        // when
        Location location = sut.writeAsync(buffer);

        // and
        sut.close();
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);

        // then
        ByteBuffer readContentBuffer = sut.read(createJournalByteBuffer(64), location);
        String readContent = readAsUtf8(readContentBuffer);
        assertThat(readContent)
            .isEqualTo(content);
    }
}
