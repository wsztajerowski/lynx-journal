package pl.wsztajerowski.journal.records;

import java.util.concurrent.locks.Condition;

public record WriteResult(long location, Condition condition) {
}
