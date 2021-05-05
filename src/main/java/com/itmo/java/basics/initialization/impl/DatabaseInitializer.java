package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;

public class DatabaseInitializer implements Initializer {
    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        File dir = initialContext.currentDbContext().getDatabasePath().toFile();
        if (!dir.exists()) {
            throw new DatabaseException(String.format("Directory with path %s is not exist", dir.getPath()));
        }
        if (!dir.canRead()){
            throw new DatabaseException(String.format("Error with reading files from directory with path %s",
                    dir.getPath()));
        }

        InitializationContextImpl.InitializationContextImplBuilder contextBuilder =
                InitializationContextImpl.builder()
                        .executionEnvironment(initialContext.executionEnvironment())
                        .currentDatabaseContext(initialContext.currentDbContext())
                        .currentSegmentContext(initialContext.currentSegmentContext());
        for (File item : dir.listFiles()) {
            if (item.isDirectory()) {
                TableInitializationContextImpl tableContext =
                        new TableInitializationContextImpl(item.getName(), dir.toPath(), new TableIndex());
                InitializationContextImpl newContext = contextBuilder.currentTableContext(tableContext).build();
                tableInitializer.perform(newContext);
            }
        }
        initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
    }
}
