package com.itmo.java.basics.config;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    private final String workingPath;

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath;
    }

    public DatabaseConfig() {
        workingPath = DEFAULT_WORKING_PATH;
    }

    public String getWorkingPath() {
        return workingPath;
    }
}
