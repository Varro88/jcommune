package org.jtalks.jcommune.model.discourse;

import java.time.LocalDateTime;

public class DiscourseCategory {
    private int id;
    private String name;
    private String description;
    private int topicCount;
    private int latestPostId;
    private int latestTopicId;
    private int position;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public DiscourseCategory(MigrationBranch branch) {
        id = (int)branch.getId();
        name = branch.getName();
        description = branch.getDescription();
        latestPostId = (int)branch.getLastPost().getId();
        topicCount = branch.getTopicInBranchCount();
        latestTopicId = branch.getLatestTopicId();
        position = branch.getPosition();
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTopicCount() {
        return topicCount;
    }

    public void setTopicCount(int topicCount) {
        this.topicCount = topicCount;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getLatestPostId() {
        return latestPostId;
    }

    public void setLatestPostId(int latestPostId) {
        this.latestPostId = latestPostId;
    }

    public int getLatestTopicId() {
        return latestTopicId;
    }

    public void setLatestTopicId(int latestTopicId) {
        this.latestTopicId = latestTopicId;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
