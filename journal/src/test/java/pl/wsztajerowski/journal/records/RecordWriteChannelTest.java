package pl.wsztajerowski.journal.records;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import pl.wsztajerowski.journal.Journal;
import pl.wsztajerowski.journal.Location;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseHexString;
import static pl.wsztajerowski.journal.BytesTestUtils.toUtf8HexString;
import static pl.wsztajerowski.journal.FilesTestUtils.wrapInJournalByteBuffer;
import static pl.wsztajerowski.journal.records.RecordTestUtils.recordHeaderPrefixInHexString;

class RecordWriteChannelTest {
    private RecordWriteChannel sut;
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("journal", ".dat");
        sut = RecordWriteChannel.open(dataFilePath, new DoubleBatch(Journal.BATCH_SIZE));
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.close();
    }

    @Disabled //FIXME: adjust the test to current impl
    @Test
    void journalWithSavedVariableContainsCorrectRecord() throws IOException {
        // given
        var content = "test";
        var buffer = wrapInJournalByteBuffer(content);
        CompletableFuture<Location> future = new CompletableFuture<>();

        // when
//        ByteBuffer[] buffers = List.of(new RecordWriteTask(buffer.getWritableBuffer(), future));
//        long writtenBytes = sut.fileChannel.write(buffers);

        // then
        future.join();
        assertThat(readAllBytes(dataFilePath))
            .asHexString()
            .containsSequence(
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(content.length()),
                toUpperCaseHexString(ChecksumCalculator.computeChecksum(content)),
                toUtf8HexString(content));
    }

}