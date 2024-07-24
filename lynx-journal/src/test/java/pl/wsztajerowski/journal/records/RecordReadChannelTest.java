package pl.wsztajerowski.journal.records;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.wsztajerowski.journal.Location;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static org.assertj.core.api.Assertions.assertThat;

class RecordReadChannelTest {
    private RecordReadChannel sut;
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("journal", ".dat");
        FileChannel readerChannel = FileChannel.open(dataFilePath, CREATE, READ);
        sut = RecordReadChannel.open(readerChannel);
    }

    @AfterEach
    void tearDown() throws IOException {
        sut.close();
    }

    @Test
    void readFromCorruptedJournalThrowsException() throws IOException {
        // given
        Files.writeString(dataFilePath, "Some text in bytes");
        var location = new Location(0);

        // when
        Exception exception = Assertions.catchException(() -> sut.read(ByteBuffer.allocate(256), location));

        // then
        assertThat(exception)
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Invalid record header format");
    }

    @Test
    void readOutsideOfJournal() {
        // given
        Location location = new Location(1000);

        // when
        Exception exception = Assertions.catchException(() -> sut.read(ByteBuffer.allocate(64), location));

        // then
        assertThat(exception)
            .isInstanceOf(IOException.class)
            .hasMessageContaining("Invalid record header format");
    }

}