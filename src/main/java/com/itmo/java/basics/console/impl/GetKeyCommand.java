package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для чтения данных по ключу
 */
public class GetKeyCommand implements DatabaseCommand {

    private static final int ARGUMENTS_QUANTITY = 5;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.env = env;
        this.commandArgs = commandArgs;
        if (commandArgs.size() != ARGUMENTS_QUANTITY) {
            throw new IllegalArgumentException(String.format("Wrong number of arguments - you need %d but given %d", ARGUMENTS_QUANTITY, commandArgs.size()));
        }
    }

    /**
     * Читает значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с прочитанным значением. Например, "previous". Null, если такого нет
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            Optional<Database> database = env.getDatabase(databaseName);
            if (database.isEmpty()) {
                throw new DatabaseException(String.format("There is no database with name %s", databaseName));
            }
            Optional<byte[]> value = database.get().read(tableName, key);
            return DatabaseCommandResult.success(value.isEmpty() ? null : value.get());
        } catch (DatabaseException e) {
            return new FailedDatabaseCommandResult(e.getMessage());
        }
    }
}
