package com.example.binloglistener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

@Component
public class BinLogListener implements CommandLineRunner {

    private static final Set<String> LISTENER_TABLE_SET = new HashSet<>();
    private static final Map<Long, String> TABLE_MAP = new HashMap<>();
    Logger logger = LoggerFactory.getLogger(this.getClass());
    ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    void init() {
        LISTENER_TABLE_SET.add("interface_config");
        LISTENER_TABLE_SET.add("system_config_properties");
        LISTENER_TABLE_SET.add("customer_config_properties");
        LISTENER_TABLE_SET.add("drools_config_properties");
    }

    @Override
    public void run(String... args) throws Exception {
        BinaryLogClient client = new BinaryLogClient("artemis", "root", "123456");
        client.setServerId(1);

        client.registerEventListener(event -> {
            EventData data = event.getData();
            if (data instanceof TableMapEventData) {
                TableMapEventData tableMapEventData = (TableMapEventData) data;
                TABLE_MAP.put(tableMapEventData.getTableId(), tableMapEventData.getTable());
            }
            if (data instanceof UpdateRowsEventData) {
                String tableName = TABLE_MAP.get(((UpdateRowsEventData) data).getTableId());
                if (!LISTENER_TABLE_SET.contains(tableName)) {
                    return;
                }
                logger.info("Update:{}\n{}", tableName, data);
            } else if (data instanceof WriteRowsEventData) {
                String tableName = TABLE_MAP.get(((WriteRowsEventData) data).getTableId());
                if (!LISTENER_TABLE_SET.contains(tableName)) {
                    return;
                }
                logger.info("Insert:{}\n{}", tableName, data);
            } else if (data instanceof DeleteRowsEventData) {
                String tableName = TABLE_MAP.get(((DeleteRowsEventData) data).getTableId());
                if (!LISTENER_TABLE_SET.contains(tableName)) {
                    return;
                }
                logger.info("Delete:{}\n{}", tableName, data);
            }
        });

        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
