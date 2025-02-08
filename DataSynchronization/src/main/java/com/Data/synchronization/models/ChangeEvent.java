package com.Data.synchronization.models;


import jakarta.persistence.Id;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

@Data
public class ChangeEvent {
    @Id
    private String id;
    private String sourceDb;
    private String targetDb;
    private String tableName;
    private ChangeType changeType;
    private Map<String, Object> data;
    private Map<String, Object> oldData;
    private String checksum;
    private Instant timestamp;
    private String status;
    private int retryCount;
}
