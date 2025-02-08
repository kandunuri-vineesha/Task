package com.Data.synchronization.service;

import com.Data.synchronization.models.DatabaseConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CDCService {

    private final ChangeEventRepository changeEventRepository;
    private final DatabaseConfigService databaseConfigService;

    public CDCService(ChangeEventRepository changeEventRepository,
                      DatabaseConfigService databaseConfigService) {
        this.changeEventRepository = changeEventRepository;
        this.databaseConfigService = databaseConfigService;
    }

    public void captureChanges(String dbName) {
        DatabaseConfig config = databaseConfigService.getConfig(dbName);
        switch (config.getType()) {
            case MYSQL -> captureMySQLChanges(config);
            case POSTGRESQL -> capturePostgresChanges(config);
            case MONGODB -> captureMongoChanges(config);
        }
    }

    private void captureMySQLChanges(DatabaseConfig config) {

    }


    private void captureMongoChanges(DatabaseConfig config) {
        // Implement Mongo change streams
    }
}
