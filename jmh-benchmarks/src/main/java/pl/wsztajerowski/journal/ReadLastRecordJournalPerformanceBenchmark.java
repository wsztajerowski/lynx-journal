package pl.wsztajerowski.journal;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Control;
import pl.wsztajerowski.journal.records.Record;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static java.nio.file.Files.createTempFile;
import static pl.wsztajerowski.journal.JournalByteBufferFactory.createJournalByteBuffer;

@State(Scope.Benchmark)
public class ReadLastRecordJournalPerformanceBenchmark {

    Journal journal;
    Path dataFilePath;
    MpmcUnboundedXaddArrayQueue<Location> queue;

    @Setup
    public void setup() throws IOException {
        dataFilePath = createTempFile("jmh-journal", ".dat");
        journal = Journal.open(dataFilePath, false);
        queue = new MpmcUnboundedXaddArrayQueue<>(1000);
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
    @Group("journal_mpmc")
    public Location produceElement(ThreadScopeState threadScopeState) {
        JournalByteBuffer buffer = threadScopeState.buffer;
        ByteBuffer input = buffer.getContentBuffer();
        input.clear();
        input.putInt(41);
        input.flip();
        Location location =  journal
            .write(buffer);
        queue.offer(location);
//        System.out.printf("Produced %s (%d-8)/16=%d%n", location, location.offset(),(location.offset() - 8) / 16);
        return location;
    }

    @Benchmark
    @GroupThreads(5)
    @Group("journal_mpmc")
    public Record consumeElement(ThreadScopeState threadScopeState, Control control) {
        ByteBuffer output = threadScopeState.buffer.getContentBuffer();
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
