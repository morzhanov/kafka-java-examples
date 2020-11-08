package com.example.FileSinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.FileSinkConnector.FileSinkConnectorConfig.API_URL_CONFIG;
import static com.example.FileSinkConnector.FileSinkConnectorConfig.TOPIC_CONFIG;
import static com.example.FileSinkConnector.FileSinkConnectorConfig.SLEEP_CONFIG;

public class FileSinkConnector extends SinkConnector {

    private FileSinkConnectorConfig fsConfig;

    @Override
    public String version() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConfigDef config() {
        return FileSinkConnectorConfig.conf();
    }

    @Override
    public void start(Map<String, String> props) {
        fsConfig = new FileSinkConnectorConfig(props);
    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>(3);
            config.put(API_URL_CONFIG, fsConfig.getString(API_URL_CONFIG));
            config.put(SLEEP_CONFIG, Integer.toString(fsConfig.getInt(SLEEP_CONFIG)));
            config.put(TOPIC_CONFIG, fsConfig.getString(TOPIC_CONFIG));
            configs.add(config);
        }
        return configs;
    }
}
