package pl.wsztajerowski.journal;

import org.openjdk.jmh.annotations.*;
import pl.wsztajerowski.journal.records.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static pl.wsztajerowski.journal.JournalByteBufferFactory.createJournalByteBuffer;

@State(Scope.Benchmark)
public class RoundTripJournalPerformanceBenchmark {

    Journal journal;
    Path dataFilePath;

    @Setup
    public void setup() throws IOException {
        dataFilePath = createTempFile("jmh-journal", ".dat");
        journal = Journal.open(dataFilePath, false);
    }

    @TearDown
    public void tearDown() {

    }

    @State(Scope.Thread)
    public static class ThreadScopeState {
        JournalByteBuffer buffer;

        @Setup
        public void setup() {
            buffer = createJournalByteBuffer(4);
        }
    }

    @Benchmark
    @GroupThreads(5)
    @Group("journal_roundtrip")
    public Record readYourWrite(ThreadScopeState threadScopeState) {
        JournalByteBuffer buffer = threadScopeState.buffer;
        ByteBuffer input = buffer.getContentBuffer();
        input.clear();
        input.putInt(41);
        input.flip();
        Location location =  journal
            .write(buffer);
        input.clear();
        return journal
            .readRecord(input, location);
    }

}
