package pl.wsztajerowski.journal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.FilesTestUtils.*;

class ReadYourOwnWritesTest {
    private Journal sut;

    @BeforeEach
    void setUp() throws IOException {
        Path dataFilePath = createTempFile("journal", ".dat");
        sut = Journal.open(dataFilePath, false);
    }

    @AfterEach
    void tearDown() {
        sut.closeJournal();
    }

    @Test
    void readYourSingleWrite() {
        // given
        var content = "Hello World";
        var buffer = wrapInByteBuffer(content);

        // when
        var location = sut.write(buffer);
        // and
        var readContentBuffer = sut.read(ByteBuffer.allocate(64), location);

        // then
        var readContent = readAsUtf8(readContentBuffer);
        assertThat(readContent)
            .isEqualTo(content);
    }

    @Test
    void readYourMultipleWrites() {
        // given
        var firstVariableContent = "My";
        var secondVariableContent = "fantastic";
        var thirdVariableContent = "project!";

        // when
        var firstVariableLocation = sut.write(wrapInByteBuffer(firstVariableContent));
        var secondVariableLocation = sut.write(wrapInByteBuffer(secondVariableContent));
        var thirdVariableLocation = sut.write(wrapInByteBuffer(thirdVariableContent));
        // and
        var secondReadContent = readAsUtf8(sut.read(ByteBuffer.allocate(64), secondVariableLocation));
        var firstReadContent = readAsUtf8(sut.read(ByteBuffer.allocate(64), firstVariableLocation));
        var thirdReadContent = readAsUtf8(sut.read(ByteBuffer.allocate(64), thirdVariableLocation));

        // then
        assertThat(String.join(" ", firstReadContent, secondReadContent, thirdReadContent))
            .isEqualTo("My fantastic project!");
    }
}
