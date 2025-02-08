package com.Data.synchronization.health;

import com.Data.synchronization.models.DatabaseConfig;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class DatabaseHealthIndicator implements HealthIndicator {

    private final DatabaseConfigService databaseConfigService;

    @Override
    public Health health() {
        try {
            checkDatabases();
            return Health.up().build();
        } catch (Exception e) {
            return Health.down()
                    .withException(e)
                    .build();
        }
    }

    private void checkDatabases() {
        databaseConfigService.getActiveConfigs()
                .forEach(this::checkConnection);
    }

    private void checkConnection(DatabaseConfig config) {

    }
}
