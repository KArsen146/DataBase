package com.itmo.java.basics.logic.impl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SegmentImpl implements Segment {
    private final String name;
    private final Path path;
    private boolean isReadonly;
    private final SegmentIndex index;
    private static final int MAX_SIZE = 100000;
    private long size;
    private final DatabaseOutputStream outputStream;

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        if (context.getCurrentSize() < MAX_SIZE){
            try {
                DatabaseOutputStream outputStream = new DatabaseOutputStream(new FileOutputStream(context.getSegmentPath().toString(), true));
                return new SegmentImpl(context.getSegmentName(), context.getSegmentPath(), context.getCurrentSize(),
                        context.getIndex(), outputStream, false);
            }
            catch (IOException ignored){
            }
        }
        return new SegmentImpl(context.getSegmentName(), context.getSegmentPath(), context.getCurrentSize(),
                context.getIndex(), null, true);
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path path = Paths.get(tableRootPath.toString(), segmentName);
        DatabaseOutputStream stream;

        /*
          Здесь я не использую try with resources, потому что я оставляю открытым поток на запись в актуальном сегменте
         */

        try {
            Files.createFile(path);
            stream = new DatabaseOutputStream(new FileOutputStream(
                    path.toString(), true));
            return new SegmentImpl(segmentName, path, stream);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    private SegmentImpl(String segmentName, Path path, DatabaseOutputStream stream) {
        name = segmentName;
        this.path = path;
        size = 0;
        index = new SegmentIndex();
        isReadonly = false;
        outputStream = stream;
    }

    private SegmentImpl(String segmentName, Path path, long size, SegmentIndex index, DatabaseOutputStream stream, boolean isReadonly){
        name = segmentName;
        this.path = path;
        this.size = size;
        this.index = index;
        this.outputStream = stream;
        this.isReadonly = isReadonly;
    }
    static String createSegmentName(String tableName) {
        try {
            Thread.sleep(1);
        }
        catch (Exception ignored){
        }
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (isReadOnly()) {
            return false;
        }
        if (objectValue == null) {
            return delete(objectKey);
        }
        try {
            long offsetSize = outputStream.write(new SetDatabaseRecord(objectKey.getBytes(), objectValue));
            index.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
            size += offsetSize;
            if (size >= MAX_SIZE) {
                isReadonly = true;
                outputStream.close();
            }
            return true;
        } catch (IOException e) {
            outputStream.close();
            throw e;
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> offsetInfo = index.searchForKey(objectKey);
        if (offsetInfo.isEmpty()) {
            return Optional.empty();
        }
        try (DatabaseInputStream stream = new DatabaseInputStream(new FileInputStream(path.toString()))) {
            long offset = offsetInfo.get().getOffset();
            long realOffset = stream.skip(offset);
            if (realOffset != offset) {
                throw new IOException("Something went wrong with stream.skip()");
            }
            Optional<DatabaseRecord> record = stream.readDbUnit();
            return record.map(DatabaseRecord::getValue);
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadonly;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (isReadOnly()) {
            return false;
        }
        try {
            long offsetSize = outputStream.write(new RemoveDatabaseRecord(objectKey.getBytes()));
            index.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
            size += offsetSize;
            if (size >= MAX_SIZE) {
                isReadonly = true;
                outputStream.close();
            }
            return true;
        } catch (IOException e) {
            outputStream.close();
            throw e;
        }
    }
}