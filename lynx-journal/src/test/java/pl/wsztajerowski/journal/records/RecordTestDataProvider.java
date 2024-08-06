package pl.wsztajerowski.journal.records;

import java.io.IOException;

public interface RecordTestDataProvider {
    long saveVariable(String variable) throws IOException;

    long saveVariable(int variable) throws IOException;

    long saveVariableWithInvalidRecordHeader(String variable) throws IOException;
}
