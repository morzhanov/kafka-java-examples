package com.example.kafka.course.events;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;

public class ConsumerProcessor implements Runnable {

    private KafkaConsumer<String, String> consumer;
    private String groupId;
    private String eventsFileName = "events.txt";

    public ConsumerProcessor(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        this.consumer = consumer;
        this.groupId = groupId;

        if (groupId.equals("1")) {
            new File(eventsFileName);
        }

        try {
            File myObj = new File("events.txt");
            if (myObj.createNewFile()) {
                System.out.println("events file created: " + myObj.getName());
            } else {
                System.out.println("events file already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred during file creation");
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            consumer.subscribe(Arrays.asList(KafkaService.TOPIC));
            System.out.println("Added new consumer to group" + groupId);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    if (groupId.equals("1")) {
                        this.processToFile(record);
                    } else {
                        this.processToStdout(record);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void processToFile(ConsumerRecord<String, String> record) {
        try {
            byte[] data = new String(record.value() + "\n").getBytes();
            Files.write(Paths.get(eventsFileName), data, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    private void processToStdout(ConsumerRecord<String, String> record) {
        String value = record.value();
        try {
            JSONObject jsonObject = new JSONObject(value);
            if (jsonObject.getString("type").equals("PushEvent")) {
                System.out.println(value);
            }
        } catch (JSONException err) {
            System.out.println(err.getMessage());
        }
    }
}
