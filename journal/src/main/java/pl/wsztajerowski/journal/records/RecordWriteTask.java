package pl.wsztajerowski.journal.records;

import pl.wsztajerowski.journal.Location;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public record RecordWriteTask(ByteBuffer byteBuffer, CompletableFuture<Location> future) {
    public void completeExceptionally(Exception e) {
        this.future.completeExceptionally(e);
    }
}
