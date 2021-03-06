package com.example.kafka.course.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubEvent {
  public String id;
  public String type;
  public Actor actor;
  public Repo repo;
  public String created_at;
}
