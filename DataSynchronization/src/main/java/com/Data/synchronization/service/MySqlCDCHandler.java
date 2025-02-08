package com.Data.synchronization.service;

import com.Data.synchronization.models.ChangeEvent;
import com.Data.synchronization.models.ChangeType;
import com.Data.synchronization.models.DatabaseConfig;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MySQLCDCHandler {
    private final ChangeEventRepository changeEventRepository;
    private final Map<Long, String> tableMap = new ConcurrentHashMap<>();
    private final BinaryLogClient client;

    public MySQLCDCHandler(DatabaseConfig config, ChangeEventRepository repository) {
        this.changeEventRepository = repository;
        this.client = new BinaryLogClient(
                config.getUrl().split("//")[1].split("/")[0],
                config.getUsername(),
                config.getPassword()
        );

        client.registerEventListener(event -> {
            EventHeader header = event.getHeader();
            EventData data = event.getData();

            if (data instanceof TableMapEventData) {
                TableMapEventData tableData = (TableMapEventData) data;
                tableMap.put(tableData.getTableId(), tableData.getTable());
            } else if (data instanceof WriteRowsEventData) {
                handleInsert((WriteRowsEventData) data);
            } else if (data instanceof UpdateRowsEventData) {
                handleUpdate((UpdateRowsEventData) data);
            } else if (data instanceof DeleteRowsEventData) {
                handleDelete((DeleteRowsEventData) data);
            }
        });
    }

    private void handleInsert(WriteRowsEventData eventData) {
        String tableName = tableMap.get(eventData.getTableId());
        eventData.getRows().forEach(row -> {
            ChangeEvent change = new ChangeEvent();
            change.setTableName(tableName);
            change.setChangeType(ChangeType.INSERT);
            change.setData(convertToMap(row));
            change.setTimestamp(Instant.now());
            change.setStatus("PENDING");
            changeEventRepository.save(change);
        });
    }

    private void handleUpdate(UpdateRowsEventData eventData) {
        String tableName = tableMap.get(eventData.getTableId());
        eventData.getRows().forEach(row -> {
            ChangeEvent change = new ChangeEvent();
            change.setTableName(tableName);
            change.setChangeType(ChangeType.UPDATE);
            change.setData(convertToMap(row.getValue()));
            change.setOldData(convertToMap(row.getKey()));
            change.setTimestamp(Instant.now());
            change.setStatus("PENDING");
            changeEventRepository.save(change);
        });
    }

    private void handleDelete(DeleteRowsEventData eventData) {
        String tableName = tableMap.get(eventData.getTableId());
        eventData.getRows().forEach(row -> {
            ChangeEvent change = new ChangeEvent();
            change.setTableName(tableName);
            change.setChangeType(ChangeType.DELETE);
            change.setData(convertToMap(row));
            change.setTimestamp(Instant.now());
            change.setStatus("PENDING");
            changeEventRepository.save(change);
        });
    }

    private Map<String, Object> convertToMap(Object[] row) {
        Map<String, Object> result = new HashMap<>();
        // Map column values to names based on table schema
        return result;
    }

    public void start() {
        try {
            client.connect();
        } catch (Exception e) {
            log.error("Failed to start MySQL CDC", e);
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            client.disconnect();
        } catch (Exception e) {
            log.error("Failed to stop MySQL CDC", e);
        }
    }
}
