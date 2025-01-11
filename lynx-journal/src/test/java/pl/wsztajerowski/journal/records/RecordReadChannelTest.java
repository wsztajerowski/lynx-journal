package pl.wsztajerowski.journal.records;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import pl.wsztajerowski.journal.JournalRuntimeIOException;
import pl.wsztajerowski.journal.Location;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;
import static pl.wsztajerowski.journal.FilesTestUtils.readAsUtf8;
import static pl.wsztajerowski.journal.JournalTestDataProvider.validJournal;
import static pl.wsztajerowski.journal.records.ByteBufferFactory.newJournalBuffer;
import static pl.wsztajerowski.journal.records.JournalByteBufferFactory.createJournalByteBuffer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RecordReadChannelTest {
    private RecordReadChannel sut;
    private Path dataFilePath;

    @BeforeAll
    static void setup() {
        Assertions.setMaxStackTraceElementsDisplayed(30);
    }

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
        Exception exception = catchException(() -> sut.read(createJournalByteBuffer(64), location));

        // then
        assertThat(exception)
            .isInstanceOf(InvalidRecordHeaderException.class)
            .hasMessageContaining("Invalid record header prefix");
    }

    @Test
    void read_outside_of_journal_throws_exception() {
        // given
        Location location = new Location(1000);

        // when
        Exception exception = catchException(() -> sut.read(createJournalByteBuffer(64), location));

        // then
        assertThat(exception)
            .isInstanceOf(JournalRuntimeIOException.class)
            .hasCauseInstanceOf(EOFException.class)
            .hasMessageContaining("Corrupted journal file");
    }

    @Test
    void read_record_bigger_than_page_size_return_expected_content() throws IOException {
        // given
        String content = "7".repeat(1_000_000);
        long offset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariable(content);
        var location = new Location(offset);
        JournalByteBuffer outputBuffer = createJournalByteBuffer(1_000_000);

        // when
        var record = sut.read(outputBuffer, location);

        // then
        assertThat(readAsUtf8(record.buffer()))
            .contains(content);
    }

    static Stream<JournalByteBuffer> invalidBuffersSource() {
        return Stream.of(
            newJournalBuffer(0, 2, 2),
            newJournalBuffer(10, 15, 20),
            newJournalBuffer(15, 20, 20)
        );
    }

    @ParameterizedTest
    @MethodSource("invalidBuffersSource")
    void read_record_providing_buffer_without_enough_space_throws_exception(JournalByteBuffer userBuffer) throws IOException {
        // given
        var variableOffset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariable("INIT Value");

        // when
        Exception exception = catchException(() -> sut.read(userBuffer, new Location(variableOffset)));

        // then
        assertThat(exception)
            .isInstanceOf(NotEnoughSpaceInBufferException.class);
    }

    static Stream<JournalByteBuffer> validReadBuffersSource(){
        return Stream.of(
            newJournalBuffer(0, 64, 64),
            newJournalBuffer(0, 32, 64),
            newJournalBuffer(16, 64, 64),
            newJournalBuffer(16, 32, 64)
        );
    }

    @ParameterizedTest
    @MethodSource("validReadBuffersSource")
    void read_record_provided_bigger_buffer_succeed(JournalByteBuffer outputBuffer) throws IOException {
        // given
        long offset = validJournal(dataFilePath)
            .recordTestDataProvider()
            .saveVariable("Test value");
        var location = new Location(offset);

        // when
        var record = sut.read(outputBuffer, location);

        // then
        assertThat(record.buffer().remaining())
            .isEqualTo(record.recordHeader().variableSize());
        // and
        assertThat(readAsUtf8(record.buffer()))
            .contains("Test value");
    }

}