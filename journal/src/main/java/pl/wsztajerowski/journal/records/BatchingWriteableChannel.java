package pl.wsztajerowski.journal.records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BatchingWriteableChannel {
    private static final ThreadFactory WRITE_CHANNEL_THREAD_FACTORY = Thread.ofPlatform()
        .name("write-channel-executor-", 1)
        .factory();
    private final ExecutorService executorService;
    private final FileChannel writeChannel;
    private final IOWritesBatch batchA;
    private final IOWritesBatch batchB;
    private final Condition batchIsFullCondition;
    private volatile IOWritesBatch currentBatch;
    private final ReentrantLock lock;
    private volatile boolean isClosed;


    public static BatchingWriteableChannel open(Path journalFile, int batchSize) throws IOException {
        ExecutorService executor = Executors.newSingleThreadExecutor(WRITE_CHANNEL_THREAD_FACTORY);
        FileChannel writerChannel = FileChannel.open(journalFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        AtomicLong virtualFileChannelPosition = new AtomicLong(writerChannel.size());
        ReentrantLock reentrantLock = new ReentrantLock(true);
        IOWritesBatch batchA = new IOWritesBatch(ByteBuffer.allocateDirect(batchSize), virtualFileChannelPosition, reentrantLock.newCondition());
        IOWritesBatch batchB = new IOWritesBatch(ByteBuffer.allocateDirect(batchSize), virtualFileChannelPosition, reentrantLock.newCondition());

        return new BatchingWriteableChannel(executor, writerChannel, batchA, batchB, reentrantLock);
    }

    private BatchingWriteableChannel(ExecutorService executor, FileChannel writeChannel, IOWritesBatch batchA, IOWritesBatch batchB, ReentrantLock lock) {
        this.executorService = executor;
        this.writeChannel = writeChannel;
        this.lock = lock;
        this.batchIsFullCondition = lock.newCondition();
        this.batchA = batchA;
        this.batchB = batchB;
        currentBatch = batchA;
        executorService.submit(this::runBatchConsumer);
    }

    public long write(ByteBuffer buffer, boolean waitForFlush) {
        if (isClosed) {
            throw new IllegalStateException("DoubleBatch is closed");
        }
        while (true) {
            lock.lock();
            try {
                if (isClosed) {
                    throw new IllegalStateException("DoubleBatch is closed");
                }
                IOWritesBatch activeBatch = currentBatch;
                if (!activeBatch.hasRemaining(buffer.remaining())) {
                    batchIsFullCondition.signal();
                    continue;
                }
                long offset = activeBatch.write(buffer);
                if (activeBatch.isFull()) {
                    batchIsFullCondition.signal();                                  // signal(consumer) - producer still has lock
                }
                if (waitForFlush) {                                                 // async write
                    while (!activeBatch.isBatchFlushed()) {
                        activeBatch.await();
                    }
                }
                return offset;

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    private void runBatchConsumer() {
        while (!isClosed) {
            lock.lock();
            try {
                IOWritesBatch batchToFlush = currentBatch;
                while (batchToFlush.isEmpty()) {
                    batchIsFullCondition.await();
                }
                flushBatchAndSignalAllWaitingWriters(batchToFlush);
                swapBatch();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
        // flush another batch if there is something to write
        lock.lock();
        try {
            flushBatchAndSignalAllWaitingWriters(currentBatch);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private void flushBatchAndSignalAllWaitingWriters(IOWritesBatch batchToFlush) throws IOException {
        ByteBuffer writableBuffer = batchToFlush.writableBuffer();
        int expectedBytesToWrite = writableBuffer.limit();
        int bytesWritten = writeChannel.write(writableBuffer);
        if (bytesWritten != expectedBytesToWrite) {
            throw new RuntimeException("Written bytes mismatch - expected: " + expectedBytesToWrite + ", actual: " + bytesWritten);
        }
        batchToFlush.clear();
        batchToFlush.markBatchAsFlushed();
        batchToFlush.signalAll();
    }

    private void swapBatch() {
        IOWritesBatch nextBatch = currentBatch == batchA ? batchB : batchA;
        nextBatch.resetFlushMark();
        currentBatch = nextBatch;
    }

    public void close() {
        this.isClosed = true;
        lock.lock();
        try {
            // force flushing
            batchIsFullCondition.signal();
        } finally {
            lock.unlock();
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
