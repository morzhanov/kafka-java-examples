package com.example.kafka.course.events;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    public static final String TOPIC = "GithubEvents";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendEvents(List<Event> events) {
        events.stream().forEach(e -> {
            ObjectMapper mapper = new ObjectMapper();
            try {
                String json = mapper.writeValueAsString(e);
                this.kafkaTemplate.send(TOPIC, json);
            } catch (JsonProcessingException err) {
                err.printStackTrace();
            }
        });
    }

    public void addConsumer(String groupId) {
        if (!groupId.equals("1") && !groupId.equals("2")) {
            throw new Error("groupId allowed values are '1' and '2'");
        }
        ConsumerProcessor processor = new ConsumerProcessor(groupId);
        Thread t = new Thread(processor);
        t.start();
    }

}
