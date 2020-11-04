package com.example.kafka.course.events;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController("/api")
class EventsController {

    @Autowired
    EventsService eventsService;

    @Autowired
    KafkaService kafkaService;

    @GetMapping("/publish")
    public List<Event> publish() {
        List<GitHubEvent> gitHubEvents = eventsService.getGitHubEvents();
        List<Event> events = eventsService.parseEvents(gitHubEvents);
        kafkaService.sendEvents(events);
        return events;
    }

    @PostMapping("/consumer")
    public String addConsumer(@RequestBody String groupId) {
        try {
            kafkaService.addConsumer(groupId);
        } catch (Error err) {
            return err.getMessage();
        }
        return "Consumer added";
    }

}
