package pl.wsztajerowski.journal.records;

public record RecordHeader(int variableSize){
    static final int RECORD_PREFIX = 0xF0CACC1A;
    private static final int NUMBER_OF_INTS_IN_HEADER = 2;

    public static int recordHeaderLength() {
        return NUMBER_OF_INTS_IN_HEADER * Integer.BYTES;
    }

}
