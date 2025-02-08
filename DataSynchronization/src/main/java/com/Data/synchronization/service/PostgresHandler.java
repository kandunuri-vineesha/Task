package com.Data.synchronization.service;

import com.Data.synchronization.models.ChangeEvent;
import com.Data.synchronization.models.ChangeType;
import com.Data.synchronization.models.DatabaseConfig;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;

@Slf4j
public class PostgresCDCHandler {
    private final ChangeEventRepository changeEventRepository;
    private final DatabaseConfig config;
    private PGReplicationStream replicationStream;
    private volatile boolean running = false;

    public PostgresCDCHandler(DatabaseConfig config, ChangeEventRepository repository) {
        this.config = config;
        this.changeEventRepository = repository;
    }

    public void start() {
        try {
            Connection conn = DriverManager.getConnection(
                    config.getUrl(),
                    config.getUsername(),
                    config.getPassword()
            );

            // Create replication slot if it doesn't exist
            createReplicationSlot(conn);

            // Start logical replication
            replicationStream = createReplicationStream(conn);
            running = true;

            // Start message processing thread
            new Thread(this::processMessages).start();
        } catch (Exception e) {
            log.error("Failed to start Postgres CDC", e);
            throw new RuntimeException(e);
        }
    }

    private void processMessages() {
        while (running) {
            try {
                ByteBuffer msg = replicationStream.readPending();
                if (msg == null) {
                    Thread.sleep(10L);
                    continue;
                }

                String change = parseLogicalReplicationMessage(msg);
                processChange(change);

                LogSequenceNumber lsn = replicationStream.getLastReceiveLSN();
                replicationStream.setFlushedLSN(lsn);
                replicationStream.setAppliedLSN(lsn);

            } catch (Exception e) {
                log.error("Error processing Postgres CDC message", e);
            }
        }
    }

    private void processChange(String changeMessage) {
        try {
            // Parse the logical replication message and create ChangeEvent
            ChangeEvent change = new ChangeEvent();
            change.setSourceDb(config.getName());
            change.setTimestamp(Instant.now());
            change.setStatus("PENDING");

            // Parse operation type and set appropriate ChangeType
            if (changeMessage.startsWith("INSERT")) {
                change.setChangeType(ChangeType.INSERT);
            } else if (changeMessage.startsWith("UPDATE")) {
                change.setChangeType(ChangeType.UPDATE);
            } else if (changeMessage.startsWith("DELETE")) {
                change.setChangeType(ChangeType.DELETE);
            }

            // Parse and set data/oldData based on the message format
            // This depends on the specific logical decoding plugin being used

            changeEventRepository.save(change);
        } catch (Exception e) {
            log.error("Error processing change message: " + changeMessage, e);
        }
    }

    public void stop() {
        running = false;
        try {
            if (replicationStream != null) {
                replicationStream.close();
            }
        } catch (Exception e) {
            log.error("Failed to stop Postgres CDC", e);
        }
    }

    private String parseLogicalReplicationMessage(ByteBuffer msg) {
        // Implement parsing logic based on the logical decoding plugin format
        return "";
    }
}