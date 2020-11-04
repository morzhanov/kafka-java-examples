package com.example.kafka.course.events;

public class Event {
    public Event(GitHubEvent gitHubEvent) {
        this.id = gitHubEvent.id;
        this.type = gitHubEvent.type;
        this.createdAt = gitHubEvent.created_at;
        this.actorId = gitHubEvent.actor.id;
        this.actorUrl = gitHubEvent.actor.url;
        this.repoId = gitHubEvent.repo.id;
        this.repoUrl = gitHubEvent.repo.url;
    }

    public String id;
    public String type;
    public String actorId;
    public String actorUrl;
    public String repoId;
    public String repoUrl;
    public String createdAt;
}
