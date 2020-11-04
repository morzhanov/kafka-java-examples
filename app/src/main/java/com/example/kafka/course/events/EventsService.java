package com.example.kafka.course.events;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class EventsService {
    public List<GitHubEvent> getGitHubEvents() {
        final String uri = "https://api.github.com/events";
        RestTemplate restTemplate = new RestTemplate();

        ResponseEntity<List<GitHubEvent>> rateResponse = restTemplate.exchange(uri, HttpMethod.GET, null,
                new ParameterizedTypeReference<List<GitHubEvent>>() {
                });
        return rateResponse.getBody();
    }

    public List<Event> parseEvents(List<GitHubEvent> events) {
        return events.stream().map(e -> new Event(e)).collect(Collectors.toList());
    }

}
