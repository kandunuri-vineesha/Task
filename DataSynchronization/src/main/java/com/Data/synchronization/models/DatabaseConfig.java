package com.Data.synchronization.models;

import lombok.Data;

@Data
public class DatabaseConfig {
    private String name;
    private String url;
    private String username;
    private String password;
    private DatabaseType type;
    private boolean active;
}
