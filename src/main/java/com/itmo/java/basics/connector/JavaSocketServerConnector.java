package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements AutoCloseable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final DatabaseServer databaseServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;
        this.serverSocket = new ServerSocket(config.getPort());
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    clientIOWorkers.submit(new ClientTask(socket, databaseServer));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        clientIOWorkers.shutdownNow();
        connectionAcceptorExecutor.shutdownNow();
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Error with closing server socket");
        }
    }


    public static void main(String[] args) throws Exception {
        DatabaseServerConfig databaseServerConfig = new ConfigLoader().readConfig();
        DatabaseServerInitializer databaseServerInitializer = new DatabaseServerInitializer(new DatabaseInitializer(new TableInitializer(new SegmentInitializer())));
        DatabaseServer databaseServer = DatabaseServer.initialize(new ExecutionEnvironmentImpl(databaseServerConfig.getDbConfig()), databaseServerInitializer);
        JavaSocketServerConnector connector = new JavaSocketServerConnector(databaseServer, databaseServerConfig.getServerConfig());
        connector.start();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;
        private final CommandReader reader;
        private final RespWriter writer;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
            try {
                this.reader = new CommandReader(new RespReader(client.getInputStream()), server.getEnv());
                this.writer = new RespWriter(client.getOutputStream());
            } catch (IOException e) {
                throw new UncheckedIOException("Error with getting client input stream", e);
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try {
                while (!client.isClosed()) {
                    DatabaseCommand command = reader.readCommand();
                    CompletableFuture<DatabaseCommandResult> databaseCommandResultCompletableFuture = server.executeNextCommand(command);
                    writer.write(databaseCommandResultCompletableFuture.get().serialize());
                }
            } catch (Exception e) {
                close();
            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                reader.close();
                writer.close();
                client.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
