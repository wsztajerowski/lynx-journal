package pl.wsztajerowski.journal;

import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Control;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.file.Files.createTempDirectory;

@State(Scope.Benchmark)
public class ReadLastRecordBaselineBenchmark {

    Journal journalIO;
    Path dataFilePath;
    MpmcUnboundedXaddArrayQueue<Location> queue;

    @Setup
    public void setup() throws IOException {
        dataFilePath = createTempDirectory("jmh-journal-io");
        journalIO = JournalBuilder.of(dataFilePath.toFile()).open();
        queue = new MpmcUnboundedXaddArrayQueue<>(1000);
    }

    @TearDown
    public void tearDown() throws IOException {
        journalIO.close();
    }

    @Benchmark
    @GroupThreads(5)
    @Group("journalio_baseline")
    public Location produceElement() throws IOException {
        var location = journalIO.write("Hello".getBytes(), Journal.WriteType.ASYNC);
        queue.offer(location);
//        System.out.printf("Produced %s (%d-8)/16=%d%n", location, location.offset(),(location.offset() - 8) / 16);
        return location;
    }

    @Benchmark
    @GroupThreads(5)
    @Group("journalio_baseline")
    public byte[] consumeElement(Control control) {
        Location location = null;
        while (!control.stopMeasurement && (location = queue.poll()) == null) {
            // active waiting
        }
        if (location == null) {
            return null;
        }
        try {
            byte[] bytes = journalIO.read(location, Journal.ReadType.ASYNC);
            if( !Arrays.equals("Hello".getBytes(), bytes)){
                throw new RuntimeException("Journal did not read expected result");
            }
            return bytes;
        } catch (IOException e) {
            System.out.printf("Reading record %s throws an exception. Data file: %s %n", location, dataFilePath);
            throw new RuntimeException(e);
        }
    }
}
