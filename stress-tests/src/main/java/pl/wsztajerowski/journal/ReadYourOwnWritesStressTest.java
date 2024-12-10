package pl.wsztajerowski.journal;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

@Outcome(id = "27, 69", expect = ACCEPTABLE, desc = "actor1 wrote record, then actor2.")
@Outcome(id = "69, 27", expect = ACCEPTABLE, desc = "actor2 wrote record, then actor1.")
@JCStressTest
@State
public class ReadYourOwnWritesStressTest {
    private final Journal journal;
    private final ByteBuffer firstInput;
    private final ByteBuffer secondInput;
    private final ByteBuffer firstOutput;
    private final ByteBuffer secondOutput;
    private final BlockingQueue<Long> queue;

    public ReadYourOwnWritesStressTest() {
        try {
            Path path = Files.createTempFile("journal-jcstress-", ".dat");
//            Path path = Path.of("jcstress-journal.dat");
            journal = Journal.open(path, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        firstInput = ByteBuffer.allocate(4)
            .putInt(27);
        secondInput = ByteBuffer.allocate(4)
            .putInt(69);
        firstOutput = ByteBuffer.allocate(4);
        secondOutput = ByteBuffer.allocate(4);
        queue = new ArrayBlockingQueue<>(2);
    }

    @Actor
    public void actor1() {
//        firstInput.rewind();
//        var location = journal.write(firstInput);
//        queue.add(location.offset());
    }

    @Actor
    public void actor2() {
//        secondInput.rewind();
//        var location = journal.write(secondInput);
//        queue.add(location.offset());
    }

    @Arbiter
    public void arbiter(II_Result result) {
//        firstOutput.clear();
//        Long offset1 = queue.poll();
////        System.out.println("offset1 = " + offset1);
//        journal.read(firstOutput, new Location(offset1));
//        result.r1 = firstOutput.getInt();
//        secondOutput.clear();
//        Long offset2 = queue.poll();
////        System.out.println("offset2 = " + offset2);
//        journal.read(secondOutput, new Location(offset2));
//        result.r2 = secondOutput.getInt();
    }
}
