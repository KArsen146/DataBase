package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class TableImpl implements Table {

    public static Table initializeFromContext(TableInitializationContext context) {
        return null;
    }
    private final String name;
    private final Path path;
    private Segment actualSegment;
    private final TableIndex index;

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Path path = Paths.get(pathToDatabaseRoot.toString(),  tableName);
        try {
            Files.createDirectory(path);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
        return new TableImpl(tableName, path, tableIndex);
    }

    private TableImpl(String tableName, Path path, TableIndex tableIndex)
    {
        name = tableName;
        this.path = path;
        actualSegment = null;
        index = tableIndex;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            if (actualSegment == null || !actualSegment.write(objectKey, objectValue)) {
                actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), path);
            }
            actualSegment.write(objectKey, objectValue);
            index.onIndexedEntityUpdated(objectKey, actualSegment);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<byte[]> value = Optional.empty();
        if (index.searchForKey(objectKey).isPresent()) {
            try{
                value = index.searchForKey(objectKey).get().read(objectKey);
            } catch (IOException e) {
                throw new DatabaseException(e);
            }
        }
        return value;
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (index.searchForKey(objectKey).isPresent()) {
            try {
                if (actualSegment == null || !actualSegment.delete(objectKey)) {
                    actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), path);
                    actualSegment.delete(objectKey);
                    index.onIndexedEntityUpdated(objectKey, actualSegment);
                }
            } catch (IOException e) {
                throw new DatabaseException(e);
            }
        } else {
            throw new DatabaseException(String.format("There is no value for key %s",
                    objectKey));
        }
    }
}
