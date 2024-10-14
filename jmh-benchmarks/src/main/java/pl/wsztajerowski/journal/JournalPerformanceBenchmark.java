package pl.wsztajerowski.journal;

import org.openjdk.jmh.annotations.*;
import pl.wsztajerowski.journal.records.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.Files.createTempFile;

@State(Scope.Benchmark)
public class JournalPerformanceBenchmark {

    Journal journal;
    AtomicLong recordCounter;
    Path dataFilePath;

    @Setup
    public void setup() throws IOException {
        dataFilePath = createTempFile("jmh-journal", ".dat");
        journal = Journal.open(dataFilePath, false);
        recordCounter = new AtomicLong(1);
    }

    @TearDown
    public void tearDown() {

    }

    @State(Scope.Thread)
    public static class ThreadScopeState {
        ByteBuffer buffer;

        @Setup
        public void setup() {
            buffer = ByteBuffer.allocate(4);
        }
    }

    @Benchmark
    @GroupThreads(1)
    @Group("g1")
    public Location produceElement(ThreadScopeState threadScopeState) {
        Long counter = recordCounter.incrementAndGet();
        ByteBuffer input = threadScopeState.buffer;
        input.clear();
        input.putInt(counter.intValue());
        input.flip();
        Location location = journal
            .write(input);
//        System.out.printf("Produced %s (%d-8)/16=%d%n", location, location.offset(),(location.offset() - 8) / 16);
        return location;
    }

    @Benchmark
    @GroupThreads(1)
    @Group("g1")
    public Record consumeElement(ThreadScopeState threadScopeState) {
        ByteBuffer output = threadScopeState.buffer;
        output.clear();
        long recordCount = recordCounter.get();
        long recordNo = recordCount > 3 ? ThreadLocalRandom.current().nextLong(recordCount - 2) : 0;
        long recordOffset = 16 * recordNo + 8;
        Location location = new Location(recordOffset);
        try {
            return journal
                .readRecord(output, location);
        } catch (JournalException e) {
            System.out.printf("Reading record %d at %s throws an exception. (Record counter: %d) Data file: %s %n", recordNo, location, recordCount, dataFilePath);
            throw e;
        }
    }

}
