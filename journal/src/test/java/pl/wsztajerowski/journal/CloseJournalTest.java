package pl.wsztajerowski.journal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.FilesTestUtils.readAsUtf8;
import static pl.wsztajerowski.journal.FilesTestUtils.wrapInJournalByteBufferWithSize;
import static pl.wsztajerowski.journal.records.JournalByteBufferFactory.createJournalByteBuffer;

class CloseJournalTest {
    private Journal sut;
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("journal", ".dat");
        sut = Journal.open(dataFilePath, false, 64);
    }

    @Test
    void readYourMultipleWrites() throws IOException {
        // given
        var firstVariableLocation = sut.writeAsync(wrapInJournalByteBufferWithSize("A", 16));
        var secondVariableLocation = sut.writeAsync(wrapInJournalByteBufferWithSize("B", 16));
        var thirdVariableLocation = sut.writeAsync(wrapInJournalByteBufferWithSize("C", 16));

        // when
        sut.close();
        // and
        sut = Journal.open(dataFilePath, false, 64);
        // and
        var secondReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), secondVariableLocation));
        var firstReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), firstVariableLocation));
        var thirdReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), thirdVariableLocation));

        // then
        assertThat(String.join(" ", firstReadContent, secondReadContent, thirdReadContent))
            .containsSubsequence("A", "B", "C");
    }
}
