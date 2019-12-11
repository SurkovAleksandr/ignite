package org.apache.ignite.internal.processors.cache.distributed.constraint;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;

public class WalReader {
    static final String PAGE_SIZE = "pageSize";
    static final String BINARY_METADATA_FILE_STORE_DIR = "binaryMetadataFileStoreDir";
    static final String MARSHALLER_MAPPING_FILE_STORE_DIR = "marshallerMappingFileStoreDir";
    static final String KEEP_BINARY = "keepBinary";
    static final String WAL_DIR = "walDir";
    static final String WAL_ARCHIVE_DIR = "walArchiveDir";
    static final String RECORD_TYPES = "recordTypes";
    static final String WAL_TIME_FROM_MILLIS = "walTimeFromMillis";
    static final String WAL_TIME_TO_MILLIS = "walTimeToMillis";
    static final String RECORD_CONTAINS_TEXT = "recordContainsText";
    static final String PAGE_ID = "pageId";

    public static void main(String[] args) throws Exception {
        /*PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);
        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();*/

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder().
            pageSize(4096).
            /*binaryMetadataFileStoreDir(new File("work/binary_meta/10_122_196_72_47500/")).
            marshallerMappingFileStoreDir(new File("work/marshaller/")).*/
            keepBinary(true);
        String[] startDir = {
            "C:\\Users\\17816867\\IdeaProjects\\ignite\\forks\\work\\db\\wal\\constraint_TxRecoveryWithConcurrentRollbackTest0",
            "C:\\Users\\17816867\\IdeaProjects\\ignite\\forks\\work\\db\\wal\\constraint_TxRecoveryWithConcurrentRollbackTest1",
            "C:\\Users\\17816867\\IdeaProjects\\ignite\\forks\\work\\db\\wal\\constraint_TxRecoveryWithConcurrentRollbackTest2"};
        //List<String> dirs = F.asList(Paths.get(startDir).toFile().list());
        //Collections.shuffle(dirs);
        //for (String dir : startDir) {
            /*builder.filesOrDirs(Paths.get(startDir, dir).toAbsolutePath().toString());*/
            builder.filesOrDirs(startDir);

            //System.out.println(dir + "\n**************");

            try (WALIterator it = factory.iterator(builder)) {
                while (it.hasNextX()) {
                    // TODO current file ((StandaloneWalRecordsIterator)it).walFileDescriptors.get(((StandaloneWalRecordsIterator)it).curIdx).getAbsolutePath()
                    IgniteBiTuple<WALPointer, WALRecord> t = it.nextX();

                    //printRecord(t.get1(), t.get2());
                    if (t.get2().type() == WALRecord.RecordType.TX_RECORD || t.get2().type() == WALRecord.RecordType.ROLLBACK_TX_RECORD)
                        System.out.println(t.toString());
                }
            }
        //}
    }

    /**
     * @param pos                pos.
     * @param record             WAL record.
     * @param types              WAL record types.
     * @param recordContainsText Filter by substring in the WAL record.
     * @param pageId             pageId.
     */
    private static void printRecord(WALPointer pos, WALRecord record) {
        String recordStr = record.toString();

        System.out.println("index:" + ((FileWALPointer)record.position()).index() +
            ", fileOffset:" + ((FileWALPointer)record.position()).fileOffset() +
            ", length:" + ((FileWALPointer)record.position()).length() +
            ", next:"+(((FileWALPointer)record.position()).fileOffset()+((FileWALPointer)record.position()).length())+
            ", record:" + record.toString());

    }

}
