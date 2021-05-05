package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {
    private final String name;
    private final Path path;
    private final Map<String, Table> tables;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        name = dbName;
        path = Paths.get(databaseRoot.toString(), dbName);
        tables = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return name;
    }

    @Override
    public Path getDatabasePath() {
        return path;
    }

    @Override
    public Map<String, Table> getTables() {
        return tables;
    }

    @Override
    public void addTable(Table table) {
        tables.put(table.getName(), table);
    }
}
