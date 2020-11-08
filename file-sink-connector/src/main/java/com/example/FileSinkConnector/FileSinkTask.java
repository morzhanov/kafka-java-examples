package com.example.FileSinkConnector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class FileSinkTask extends SinkTask {

    private final String eventsFilePath = "/home/appuser/events.txt";

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            Files.createFile(Paths.get(eventsFilePath));
        } catch (IOException e) {
            System.out.println("An error occurred during file creation");
            e.printStackTrace();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        records.forEach((record) -> {
            try {
                byte[] data = new String(record.value() + "\n").getBytes();
                Files.write(Paths.get(eventsFilePath), data, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        });
    }

    @Override
    public void stop() {
    }
}
