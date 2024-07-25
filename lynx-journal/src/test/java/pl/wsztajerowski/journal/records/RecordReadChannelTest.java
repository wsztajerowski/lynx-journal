package pl.wsztajerowski.journal.records;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.exceptions.InvalidRecordHeader;
import pl.wsztajerowski.journal.exceptions.NotEnoughSpaceInBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;
import static pl.wsztajerowski.journal.JournalTestDataProvider.validJournal;

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
        long offset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariableWithInvalidRecordHeader("Test value");
        var location = new Location(offset);

        // when
        Exception exception = catchException(() -> sut.read(ByteBuffer.allocate(256), location));

        // then
        assertThat(exception)
            .isInstanceOf(InvalidRecordHeader.class)
            .hasMessageContaining("Invalid record header format");
    }

    @Test
    void readOutsideOfJournalThrowsException() {
        // given
        Location location = new Location(1000);

        // when
        Exception exception = catchException(() -> sut.read(ByteBuffer.allocate(64), location));

        // then
        assertThat(exception)
            .isInstanceOf(InvalidRecordHeader.class)
            .hasMessageContaining("Invalid record header format");
    }

    @Test
    void readRecordProvidingTooSmallBufferThrowsException() throws IOException {
        // given
        var variableOffset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariable("INIT Value");
        ByteBuffer userBuffer = ByteBuffer.allocate(2);

        // when
        Exception exception = catchException(() -> sut.read(userBuffer, new Location(variableOffset)));

        // then
        assertThat(exception)
            .isInstanceOf(NotEnoughSpaceInBuffer.class);
    }

    @Test
    void readRecordProvidingBufferWithoutEnoughSpaceThrowsException() throws IOException {
        // given
        var variableOffset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariable("INIT Value");
        ByteBuffer userBuffer = ByteBuffer.allocate(20)
            .position(10)
            .limit(15);

        // when
        Exception exception = catchException(() -> sut.read(userBuffer, new Location(variableOffset)));

        // then
        assertThat(exception)
            .isInstanceOf(NotEnoughSpaceInBuffer.class);
    }

    @Test
    void readRecordProvidingTooBigBufferThrowsException() {
    }

}