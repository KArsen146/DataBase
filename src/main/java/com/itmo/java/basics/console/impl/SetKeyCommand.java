package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {

    private static final int ARGUMENTS_QUANTITY = 6;
    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.env = env;
        this.commandArgs = commandArgs;
        if (commandArgs.size() != ARGUMENTS_QUANTITY) {
            throw new IllegalArgumentException(String.format("Wrong number of arguments - you need %d but given %d", ARGUMENTS_QUANTITY, commandArgs.size()));
        }
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            byte[] value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString().getBytes(StandardCharsets.UTF_8);
            Optional<Database> database = env.getDatabase(databaseName);
            if (database.isEmpty()) {
                throw new DatabaseException(String.format("There is no database with name %s", databaseName));
            }
            database.get().write(tableName, key, value);
            return DatabaseCommandResult.success(String.format("Key %s was successfully added in table %s in database %s", key, tableName, databaseName)
                    .getBytes(StandardCharsets.UTF_8));

        } catch (DatabaseException e) {
            return new FailedDatabaseCommandResult(e.getMessage());
        }
    }
}
