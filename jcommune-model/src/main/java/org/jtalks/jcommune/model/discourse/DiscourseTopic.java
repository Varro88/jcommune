package org.jtalks.jcommune.model.discourse;

import org.jtalks.jcommune.model.entity.Topic;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class DiscourseTopic {

    private int id;
    private String title;
    private LocalDateTime lastPostedAt;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private int postCount;
    private int views;
    private boolean pinned;
    private int userId;
    private int lastPostUserId;
    private int categoryId;
    private boolean closed;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public LocalDateTime getLastPostedAt() {
        return lastPostedAt;
    }

    public void setLastPostedAt(LocalDateTime lastPostedAt) {
        this.lastPostedAt = lastPostedAt;
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

    public int getPostCount() {
        return postCount;
    }

    public void setPostCount(int postCount) {
        this.postCount = postCount;
    }

    public int getViews() {
        return views;
    }

    public void setViews(int views) {
        this.views = views;
    }

    public boolean isPinned() {
        return pinned;
    }

    public void setPinned(boolean pinned) {
        this.pinned = pinned;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getLastPostUserId() {
        return lastPostUserId;
    }

    public void setLastPostUserId(int lastPostUserId) {
        this.lastPostUserId = lastPostUserId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public DiscourseTopic(Topic topic) {
        id = (int)topic.getId();
        title = topic.getTitle();
        lastPostedAt = DiscourseMigration.jodaToJavaLocalDateTime(topic.getModificationDate());
        createdAt = DiscourseMigration.jodaToJavaLocalDateTime(topic.getCreationDate());
        updatedAt = DiscourseMigration.jodaToJavaLocalDateTime(topic.getModificationDate());
        postCount = topic.getPostCount();
        views = topic.getViews();
        pinned = topic.isSticked();
        userId = (int)topic.getTopicStarter().getId();
        categoryId = (int)topic.getBranch().getId();
        closed = topic.isClosed();
        this.lastPostUserId = (int)topic.getLastPost().getUserCreated().getId();
    }

    public static List<DiscourseTopic> getTopics(List<Topic> jcommuneTopics) {
        List<DiscourseTopic> topics = new ArrayList<>();
        for (Topic topic : jcommuneTopics) {
            topics.add(new DiscourseTopic(topic));
        }
        return topics;
    }

    @Override
    public String toString() {
        return String.format("[%1$s] '%2$s'; ", id, title);
    }
}
