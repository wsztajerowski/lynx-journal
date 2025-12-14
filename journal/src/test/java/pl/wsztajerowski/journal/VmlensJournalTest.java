package pl.wsztajerowski.journal;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.vmlens.api.AllInterleavings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseHexString;
import static pl.wsztajerowski.journal.BytesTestUtils.toUpperCaseUtf8HexString;
import static pl.wsztajerowski.journal.FilesTestUtils.readAsUtf8;
import static pl.wsztajerowski.journal.FilesTestUtils.wrapInJournalByteBufferWithSize;
import static pl.wsztajerowski.journal.records.JournalByteBufferFactory.createJournalByteBuffer;
import static pl.wsztajerowski.journal.records.RecordTestUtils.recordHeaderPrefixInHexString;

@Disabled
public class VmlensJournalTest {
    public static final int BATCH_SIZE = 64;
    private final String content1 = "Vmlens ";
    private final String content2 = "concurrency test";
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("vmlens-journal", ".dat");
    }

    @Test
    void vmlens_concurrent_test() throws IOException, InterruptedException {

        try (AllInterleavings allInterleavings =
                 new AllInterleavings("JournalTest.shouldBeThreadSafe")) {

            while (allInterleavings.hasNext()) {
                // przygotowanie stanu dla danej iteracji
                try (Journal sut = Journal.open(dataFilePath, true, BATCH_SIZE)) {
                    Thread t1 = new Thread(() -> sut.write(wrapInJournalByteBufferWithSize(content1, BATCH_SIZE)));
                    Thread t2 = new Thread(() -> sut.write(wrapInJournalByteBufferWithSize(content2, BATCH_SIZE)));

                    t1.start();
                    t2.start();
                    t1.join();
                    t2.join();


                    assertThat(readAllBytes(dataFilePath))
                        .asHexString()
                        .containsSequence(
                            recordHeaderPrefixInHexString(),
                            toUpperCaseHexString(64),
                            toUpperCaseUtf8HexString(content1),
                            recordHeaderPrefixInHexString(),
                            toUpperCaseHexString(64),
                            toUpperCaseUtf8HexString(content2));
                }
            }
        }
    }

    @Disabled
    @Test
    void write_and_read_from_in_memory_journal() throws IOException {
        // given
        var content = "Hello World";
        var buffer = FilesTestUtils.wrapInJournalByteBufferWithSize(content, BATCH_SIZE);
        ByteBuffer readContentBuffer;

        // when
        try (FileSystem fs = Jimfs.newFileSystem(Configuration.unix())) {
            Path journalPath = fs.getPath("/test.journal");
            Files.writeString(journalPath, "test");
            try (Journal sut = Journal.open(journalPath, true, BATCH_SIZE)) {
                var location = sut.write(buffer);
                // and
                readContentBuffer = sut.read(createJournalByteBuffer(64), location);

            }
        }
        // then
        var readContent = readAsUtf8(readContentBuffer);
        assertThat(readContent)
            .startsWith(content);
    }
}
