package pl.wsztajerowski.journal.records;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseHexString;
import static pl.wsztajerowski.journal.BytesTestUtils.toUtf8HexString;
import static pl.wsztajerowski.journal.FilesTestUtils.wrapInByteBuffer;
import static pl.wsztajerowski.journal.records.RecordTestUtils.recordHeaderPrefixInHexString;

class RecordWriteChannelTest {
    private RecordWriteChannel sut;
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("journal", ".dat");
        FileChannel writerChannel = FileChannel.open(dataFilePath, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        sut = RecordWriteChannel.open(writerChannel);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.close();
    }

    @Test
    void journalWithSavedVariableContainsCorrectRecord() throws IOException {
        // given
        String content = "test";
        ByteBuffer buffer = wrapInByteBuffer(content);

        // when
        sut.append(buffer);

        // then
        assertThat(readAllBytes(dataFilePath))
            .asHexString()
            .containsSequence(
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(content.length()),
                toUtf8HexString(content));
    }

}