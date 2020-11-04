package com.example.kafka.course.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
class Actor {
  public String id;
  public String url;
}

@JsonIgnoreProperties(ignoreUnknown = true)
class Repo {
  public String id;
  public String url;
}

@JsonIgnoreProperties(ignoreUnknown = true)
public class GitHubEvent {
  public String id;
  public String type;
  public Actor actor;
  public Repo repo;
  public String created_at;
}
