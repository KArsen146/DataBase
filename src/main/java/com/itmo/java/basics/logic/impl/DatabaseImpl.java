package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String name;
    private final Path path;
    private final Map<String, Table> tables;

    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException("dbName is not stated");
        }
        if (!Files.exists(databaseRoot)) {
            throw new DatabaseException(String.format("There is no directory with path %s, there you want to create database",
                    databaseRoot.toString()));
        }
        Path path = Paths.get(databaseRoot.toString(), dbName);

        if (Files.exists(path)) {
            throw new DatabaseException(String.format("Database with path %s is already exist",
                    path.toString()));
        }

        try {
            Files.createDirectory(path);
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        return new DatabaseImpl(dbName, path);
    }

    private DatabaseImpl(String dbName, Path databaseRoot)
    {
        name = dbName;
        path = databaseRoot;
        tables = new HashMap<>();
    }

    private Table getTable(String tName) throws DatabaseException
    {
        if (!tables.containsKey(tName)) {
            throw new DatabaseException(String.format("There is no table with name %s",
                    tName));
        }
        return tables.get(tName);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table name is not stated");
        }
        if (!tables.containsKey(tableName)) {
            tables.put(tableName, TableImpl.create(tableName, path,
                    new TableIndex()));
        } else {
            throw new DatabaseException(String.format("Table with name %s already exists",
                    tableName));
        }
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        getTable(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        return getTable(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        getTable(tableName).delete(objectKey);
    }
}
