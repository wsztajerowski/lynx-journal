package pl.wsztajerowski.journal;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.ConcurrencyTest;
import pl.wsztajerowski.journal.records.RecordHeader;

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

@ExtendWith(FrayTestExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConcurrentBatchingFileChannelTest {
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

    @ConcurrencyTest
    void read_after_flushed_writes_by_closing_journal() throws IOException {
        // given
        String firstVariableContent = "My";
        String secondVariableContent = "project!";

        // when
        Location firstVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(firstVariableContent));
        Location secondVariableLocation = sut.writeAsync(wrapInJournalByteBuffer(secondVariableContent));

        // and
        sut.close();
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);

        // then
        String secondReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), secondVariableLocation));
        String firstReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), firstVariableLocation));
        assertThat(String.join(" ", firstReadContent, secondReadContent))
            .isEqualTo("My project!");
    }

    @ConcurrencyTest
    void write_buffer_with_size_of_batch_flushes_previous_writes() throws IOException {
        // given
        String firstVariableContent = "My";
        String secondVariableContent = "project!";

        // when
        sut.writeAsync(wrapInJournalByteBufferWithSize(firstVariableContent, BATCH_SIZE));
        sut.write(wrapInJournalByteBufferWithSize(secondVariableContent, BATCH_SIZE));

        // then
        assertThat(readAllBytes(dataFilePath))
            .asHexString()
            .containsSubsequence(
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(BATCH_SIZE - RecordHeader.recordHeaderLength()),
                toUpperCaseUtf8HexString(firstVariableContent),
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(BATCH_SIZE - RecordHeader.recordHeaderLength()),
                toUpperCaseUtf8HexString(secondVariableContent));
    }
}
