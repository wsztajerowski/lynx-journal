package pl.wsztajerowski.journal.records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DoubleBatch {
    private final ExecutorService executorService;
    private final FileChannel writeChannel;
    private final Batch batchA;
    private final Batch batchB;
    private final Condition batchIsFullCondition;
    private volatile Batch currentBatch;
    private final ReentrantLock batchLock;
    private volatile boolean isClosed;


    public static DoubleBatch open(Path journalFile, int batchSize) {
        try {
            ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "write-channel-executor"));
            FileChannel writerChannel = FileChannel.open(journalFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);

            return new DoubleBatch(executor, writerChannel, batchSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    DoubleBatch(ExecutorService executor, FileChannel writeChannel, int batchSize) throws IOException {
        this.executorService = executor;
        this.writeChannel = writeChannel;
        AtomicLong virtualPosition = new AtomicLong(writeChannel.position());
        batchLock = new ReentrantLock(true);
        Condition batchAHasFlushedCondition = batchLock.newCondition();
        Condition batchBHasFlushedCondition = batchLock.newCondition();
        batchIsFullCondition = batchLock.newCondition();
        batchA = new Batch(batchSize, virtualPosition, batchAHasFlushedCondition);
        batchB = new Batch(batchSize, virtualPosition, batchBHasFlushedCondition);
        currentBatch = batchA;
        executorService.submit(this::runBatchConsumer);
    }

    public long write(ByteBuffer buffer, boolean waitForFlush) {
        if (isClosed) {
            throw new IllegalStateException("DoubleBatch is closed");
        }
        while (true) {
            batchLock.lock();
            try {
                if (isClosed) {
                    throw new IllegalStateException("DoubleBatch is closed");
                }
                Batch activeBatch = currentBatch;
                if (!activeBatch.hasRemaining(buffer.remaining())) {
                    trySwapBatch();
                    continue;
                }
                long offset = activeBatch.write(buffer);
                if (activeBatch.isFull()) {
                    trySwapBatch();                                                 // signal(consumer) - producer still has lock
                }
                if (waitForFlush) {                                                 // async write
                    while (!activeBatch.hasBatchFlushed()) {
                        activeBatch.getBatchHasFlushedCondition().await();
                    }
                }
                return offset;

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                batchLock.unlock();
            }
        }
    }

    private void runBatchConsumer() {
        while (!isClosed) {
            batchLock.lock();
            try {
                Batch batchToFlush = currentBatch;
                while (batchToFlush.isEmpty()) {
                    batchIsFullCondition.await();
                }

                flushBatchAndSignalAllWaitingWriters(batchToFlush);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                batchLock.unlock();
            }
        }
        // flush another batch if there is something to write
        batchLock.lock();
        try {
            flushBatchAndSignalAllWaitingWriters(currentBatch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            batchLock.unlock();
        }
    }

    private void flushBatchAndSignalAllWaitingWriters(Batch batchToFlush) throws IOException {
        ByteBuffer writableBuffer = batchToFlush.writableBuffer();
        int expectedBytesToWrite = writableBuffer.limit();
        int bytesWritten = writeChannel.write(writableBuffer);
        if (bytesWritten != expectedBytesToWrite) {
            throw new RuntimeException("Written bytes mismatch - expected: " + expectedBytesToWrite + ", actual: " + bytesWritten);
        }
        batchToFlush.clear();
        batchToFlush.markBatchAsFlushed();
        batchToFlush.getBatchHasFlushedCondition().signalAll();
    }

    private void trySwapBatch() {
        batchIsFullCondition.signal();
        Batch nextBatch = currentBatch == batchA ? batchB : batchA;
        if (nextBatch.isEmpty()) {
            nextBatch.resetFlushMark();
            currentBatch = nextBatch;
        }
    }

    public void close() {
        this.isClosed = true;
        batchLock.lock();
        try {
            // force flushing
            batchIsFullCondition.signal();
        } finally {
            batchLock.unlock();
        }
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(500, MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
