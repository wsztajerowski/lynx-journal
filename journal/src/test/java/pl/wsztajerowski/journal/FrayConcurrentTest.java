package pl.wsztajerowski.journal;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pastalab.fray.junit.junit5.FrayTestExtension;
import org.pastalab.fray.junit.junit5.annotations.ConcurrencyTest;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;

import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseHexString;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseUtf8HexString;
import static pl.wsztajerowski.journal.FilesTestUtils.wrapInJournalByteBufferWithSize;
import static pl.wsztajerowski.journal.records.RecordTestUtils.recordHeaderPrefixInHexString;

@ExtendWith(FrayTestExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class FrayConcurrentTest {
    public static final int BATCH_SIZE = 64;
    private Journal sut;
    private Path dataFilePath;
    private FileSystem fileSystem;

    @BeforeEach
    void setUp() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        dataFilePath = fileSystem.getPath("/test-concurrent-fray.journal");
        sut = Journal.open(dataFilePath, false, BATCH_SIZE);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.close();
        fileSystem.close();
    }

    @ConcurrencyTest
    void write_buffer_with_size_of_batch_flushes_previous_writes() throws IOException {
        // given
        var firstVariableContent = "My";
        var secondVariableContent = "project!";

        // when
        var firstVariableLocation = sut.writeAsync(wrapInJournalByteBufferWithSize(firstVariableContent, BATCH_SIZE));
        var secondVariableLocation = sut.writeAsync(wrapInJournalByteBufferWithSize(secondVariableContent, BATCH_SIZE));
//
//        // and
//        var secondReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), secondVariableLocation));
//        var firstReadContent = readAsUtf8(sut.read(createJournalByteBuffer(64), firstVariableLocation));

        // then

        // then
        assertThat(readAllBytes(dataFilePath))
            .asHexString()
            .containsSequence(
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(BATCH_SIZE),
                toUpperCaseUtf8HexString(firstVariableContent),
                recordHeaderPrefixInHexString(),
                toUpperCaseHexString(BATCH_SIZE),
                toUpperCaseUtf8HexString(secondVariableContent));
//        assertThat(String.join(" ", firstReadContent, secondReadContent, thirdReadContent))
//            .isEqualTo("My fantastic project!");
    }
}
