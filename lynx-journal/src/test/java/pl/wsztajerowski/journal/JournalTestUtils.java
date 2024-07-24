package pl.wsztajerowski.journal;

import static java.lang.Integer.toHexString;
import static pl.wsztajerowski.journal.Journal.JOURNAL_PREFIX;

public class JournalTestUtils {
    public static CharSequence journalHeaderPrefixInHexString() {
        return toHexString(JOURNAL_PREFIX).toUpperCase();
    }

    public static CharSequence journalCurrentSchemaVersionInHexString() {
        return HexTestUtils.toUpperCaseHexString(Journal.SCHEMA_VERSION_V1);
    }

}
