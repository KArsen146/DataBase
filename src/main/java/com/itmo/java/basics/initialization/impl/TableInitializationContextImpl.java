package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import java.nio.file.Path;
import java.nio.file.Paths;
public class TableInitializationContextImpl implements TableInitializationContext {
    private final String name;
    private final Path path;
    private Segment actualSegment;
    private final TableIndex index;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        name = tableName;
        path = Paths.get(databasePath.toString(), tableName);
        index = tableIndex;
        actualSegment = null;
    }

    @Override
    public String getTableName() {
        return name;
    }

    @Override
    public Path getTablePath() {
        return path;
    }

    @Override
    public TableIndex getTableIndex() {
        return index;
    }

    @Override
    public Segment getCurrentSegment() {
        return actualSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        actualSegment = segment;
    }
}
