package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private static final String DEFAULT_NAME = "server.properties";
    private final String name;
    private static final String KVS_WORKING_PATH = "kvs.workingPath";
    private static final String KVS_HOST = "kvs.host";
    private static final String KVS_PORT = "kvs.port";

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        name = DEFAULT_NAME;
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        this.name = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        Properties properties = new Properties();
        properties.setProperty(KVS_HOST, ServerConfig.DEFAULT_HOST);
        properties.setProperty(KVS_PORT, String.valueOf(ServerConfig.DEFAULT_PORT));
        properties.setProperty(KVS_WORKING_PATH, DatabaseConfig.DEFAULT_WORKING_PATH);
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(name)) {
            properties.load(is);
        } catch (Exception ignored) {
        }
        try (FileInputStream inputStream = new FileInputStream(name)) {
            properties.load(inputStream);
        } catch (Exception ignored) {
        }
        return new DatabaseServerConfig(new ServerConfig(properties.getProperty(KVS_HOST),
                Integer.parseInt(properties.getProperty(KVS_PORT))),
                new DatabaseConfig(properties.getProperty(KVS_WORKING_PATH)));
    }
}
