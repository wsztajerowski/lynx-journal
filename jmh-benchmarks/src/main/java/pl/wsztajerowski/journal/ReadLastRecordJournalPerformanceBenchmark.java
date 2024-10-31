package pl.wsztajerowski.journal;

import org.jctools.queues.atomic.SpscLinkedAtomicQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Control;
import pl.wsztajerowski.journal.records.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;

@State(Scope.Benchmark)
public class ReadLastRecordJournalPerformanceBenchmark {

    Journal journal;
    Path dataFilePath;
    SpscLinkedAtomicQueue<Location> queue;

    @Setup
    public void setup() throws IOException {
        dataFilePath = createTempFile("jmh-journal", ".dat");
        journal = Journal.open(dataFilePath, false);
        queue = new SpscLinkedAtomicQueue<>();
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
        ByteBuffer input = threadScopeState.buffer;
        input.clear();
        input.putInt(41);
        input.flip();
        Location location = journal
            .write(input);
        queue.offer(location);
//        System.out.printf("Produced %s (%d-8)/16=%d%n", location, location.offset(),(location.offset() - 8) / 16);
        return location;
    }

    @Benchmark
    @GroupThreads(1)
    @Group("g1")
    public Record consumeElement(ThreadScopeState threadScopeState, Control control) {
        ByteBuffer output = threadScopeState.buffer;
        output.clear();
        Location location = null;
        while (!control.stopMeasurement && (location = queue.poll()) == null) {
            // active waiting
        }
        if (location == null) {
            return null;
        }
        try {
            return journal
                .readRecord(output, location);
        } catch (JournalException e) {
            System.out.printf("Reading record %s throws an exception. Data file: %s %n", location, dataFilePath);
            throw e;
        }
    }

}
