package com.example.kafka.course.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
class Actor {
    public String id;
    public String url;
}
