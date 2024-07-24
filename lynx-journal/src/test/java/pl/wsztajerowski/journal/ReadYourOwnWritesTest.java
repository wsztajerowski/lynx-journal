package pl.wsztajerowski.journal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;

class ReadYourOwnWritesTest {
    private Journal sut;

    @BeforeEach
    void setUp() throws IOException {
        Path dataFilePath = createTempFile("journal", ".dat");
        sut = Journal.open(dataFilePath);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.closeJournal();
    }

    @Test
    void readYourSingleWrite() throws IOException {
        // given
        var content = "Hello World";
        var buffer = FilesTestUtils.wrapInByteBuffer(content);

        // when
        var location = sut.write(buffer);
        // and
        var readContentBuffer = sut.read(ByteBuffer.allocate(64), location);

        // then
        var readContent = FilesTestUtils.readAsUtf8(readContentBuffer);
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
        var firstVariableLocation = sut.write(FilesTestUtils.wrapInByteBuffer(firstVariableContent));
        var secondVariableLocation = sut.write(FilesTestUtils.wrapInByteBuffer(secondVariableContent));
        var thirdVariableLocation = sut.write(FilesTestUtils.wrapInByteBuffer(thirdVariableContent));
        // and
        var secondReadContent = FilesTestUtils.readAsUtf8(sut.readRecord(ByteBuffer.allocate(64), secondVariableLocation).buffer());
        var firstReadContent = FilesTestUtils.readAsUtf8(sut.readRecord(ByteBuffer.allocate(64), firstVariableLocation).buffer());
        var thirdReadContent = FilesTestUtils.readAsUtf8(sut.readRecord(ByteBuffer.allocate(64), thirdVariableLocation).buffer());

        // then
        assertThat(String.join("", firstReadContent, secondReadContent, thirdReadContent))
            .isEqualTo("My fantastic project!");
    }
}
