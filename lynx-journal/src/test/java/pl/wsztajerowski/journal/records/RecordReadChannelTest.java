package pl.wsztajerowski.journal.records;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.exceptions.InvalidRecordHeader;
import pl.wsztajerowski.journal.exceptions.JournalRuntimeIOException;
import pl.wsztajerowski.journal.exceptions.NotEnoughSpaceInBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;
import static pl.wsztajerowski.journal.FilesTestUtils.readAsUtf8;
import static pl.wsztajerowski.journal.JournalTestDataProvider.validJournal;
import static pl.wsztajerowski.journal.records.ByteBufferFactory.newByteBuffer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RecordReadChannelTest {
    private RecordReadChannel sut;
    private Path dataFilePath;

    @BeforeEach
    void setUp() throws IOException {
        dataFilePath = createTempFile("journal", ".dat");
        sut = RecordReadChannel.open(dataFilePath);
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
            .isInstanceOf(JournalRuntimeIOException.class)
            .hasMessageContaining("Read from outside of channel");
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

    static Stream<ByteBuffer> validReadBuffersSource(){
        return Stream.of(
            ByteBuffer.allocate(64),
            ByteBuffer.allocate(64).limit(32),
            ByteBuffer.allocate(64).position(32),
            ByteBuffer.allocate(64).position(16).limit(48)
        );
    }

    @ParameterizedTest
    @MethodSource("validReadBuffersSource")
    void read_record_provided_bigger_buffer_succeed(ByteBuffer outputBuffer) throws IOException {
        // given
        long offset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariable("Test value");
        var location = new Location(offset);

        // when
        var record = sut.read(outputBuffer, location);

        // then
        assertThat(record.buffer().remaining())
            .isEqualTo(outputBuffer.position() + record.recordHeader().variableSize());
        // and
        assertThat(readAsUtf8(record.buffer()))
            .contains("Test value");
    }

}