package pl.wsztajerowski;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;
import pl.wsztajerowski.journal.Journal;
import pl.wsztajerowski.journal.Location;
import pl.wsztajerowski.journal.records.JournalByteBuffer;
import pl.wsztajerowski.journal.records.JournalByteBufferFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Path;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;


public class JournalStressTest {

    @Outcome(id = "1, 1", expect = ACCEPTABLE_INTERESTING, desc = "Both actors came up with the same value: atomicity failure.")
    @Outcome(id = "1, 2", expect = ACCEPTABLE,             desc = "actor1 incremented, then actor2.")
    @Outcome(id = "2, 1", expect = ACCEPTABLE,             desc = "actor2 incremented, then actor1.")
    @JCStressTest
    @State
    public static class PlainTest {
        Journal journal;
        Location firstLocation;
        Location secondLocation;
        PlainTest() throws IOException {
            try (FileSystem fs = Jimfs.newFileSystem(Configuration.unix())) {
                Path journalPath = fs.getPath("/test.journal");
                journal = Journal.open(journalPath, false);
            }
        }

        @Actor
        public void actor1() {
            JournalByteBuffer byteBuffer = JournalByteBufferFactory.createJournalByteBuffer(4);
            byteBuffer.getContentBuffer().putInt(1);
            firstLocation = journal.write(byteBuffer);
        }

        @Actor
        public void actor2() {
            JournalByteBuffer byteBuffer = JournalByteBufferFactory.createJournalByteBuffer(4);
            byteBuffer.getContentBuffer().putInt(2);
            secondLocation = journal.write(byteBuffer);
        }

        @Arbiter
        public void arbiter(II_Result r) {
            JournalByteBuffer byteBuffer = JournalByteBufferFactory.createJournalByteBuffer(4);
            ByteBuffer recordData = journal.read(byteBuffer, firstLocation);
            r.r1 = recordData.getInt();
            byteBuffer.clear();
            recordData = journal.read(byteBuffer, secondLocation);
            r.r2 = recordData.getInt();
        }
    }
}
