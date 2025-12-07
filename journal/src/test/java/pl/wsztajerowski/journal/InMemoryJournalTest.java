package pl.wsztajerowski.journal;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.wsztajerowski.journal.FilesTestUtils.readAsUtf8;
import static pl.wsztajerowski.journal.records.JournalByteBufferFactory.createJournalByteBuffer;

public class InMemoryJournalTest {


    private static final int BATCH_SIZE = 64;

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
