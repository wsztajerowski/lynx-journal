package pl.wsztajerowski.journal.records;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.exceptions.InvalidRecordHeader;
import pl.wsztajerowski.journal.exceptions.NotEnoughSpaceInBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;
import static pl.wsztajerowski.journal.JournalTestDataProvider.validJournal;
import static pl.wsztajerowski.journal.records.ByteBufferFactory.newByteBuffer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
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
    void read_corrupted_record_throws_exception() throws IOException {
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
    void read_outside_of_journal_throws_exception() {
        // given
        Location location = new Location(1000);

        // when
        Exception exception = catchException(() -> sut.read(ByteBuffer.allocate(64), location));

        // then
        assertThat(exception)
            .isInstanceOf(InvalidRecordHeader.class)
            .hasMessageContaining("Invalid record header format");
    }

    static Stream<ByteBuffer> invalidBuffersSource() {
        return Stream.of(
            newByteBuffer(0, 2, 2),
            newByteBuffer(10, 15, 20),
            newByteBuffer(15, 20, 20)
        );
    }

    @ParameterizedTest
    @MethodSource("invalidBuffersSource")
    void read_record_providing_buffer_without_enough_space_throws_exception(ByteBuffer userBuffer) throws IOException {
        // given
        var variableOffset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariable("INIT Value");

        // when
        Exception exception = catchException(() -> sut.read(userBuffer, new Location(variableOffset)));

        // then
        assertThat(exception)
            .isInstanceOf(NotEnoughSpaceInBuffer.class);
    }

}