package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;

public class DatabaseServerInitializer implements Initializer {
    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }
    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File dir = context.executionEnvironment().getWorkingPath().toFile();
        if (!dir.exists() && !dir.mkdir()) {
                throw new DatabaseException(String.format("Error with creating a directory with path %s",
                        dir.getPath()));
        }
        if (!dir.canRead()){
            throw new DatabaseException(String.format("Error with reading files from directory with path %s",
                    dir.getPath()));
        }
        InitializationContextImpl.InitializationContextImplBuilder contextBuilder =
                InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentSegmentContext(context.currentSegmentContext())
                        .currentTableContext(context.currentTableContext());
        for (File item : dir.listFiles()) {
            if (item.isDirectory()) {
                DatabaseInitializationContextImpl databaseContext =
                        new DatabaseInitializationContextImpl(item.getName(), dir.toPath());
                databaseInitializer.perform(contextBuilder.currentDatabaseContext(databaseContext).build());
            }
        }
    }
}
