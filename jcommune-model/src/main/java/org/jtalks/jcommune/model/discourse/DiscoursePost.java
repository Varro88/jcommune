package org.jtalks.jcommune.model.discourse;

import org.jtalks.jcommune.model.entity.Post;
import org.jtalks.jcommune.model.entity.PostComment;
import org.kefirsf.bb.BBProcessorFactory;
import org.kefirsf.bb.TextProcessor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class DiscoursePost {

    private int id;
    private int userId;
    private int topicId;
    private int postNumber;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private String raw;
    private String cooked;

    public static List<DiscoursePost> getPosts(List<Post> jcommunePosts) {
        List<DiscoursePost> posts = new ArrayList<>();
        for(Post jcommunePost : jcommunePosts) {
            posts.add(new DiscoursePost(jcommunePost));
        }
        return posts;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getTopicId() {
        return topicId;
    }

    public void setTopicId(int topicId) {
        this.topicId = topicId;
    }

    public int getPostNumber() {
        return postNumber;
    }

    public void setPostNumber(int postNumber) {
        this.postNumber = postNumber;
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

    public String getRaw() {
        return raw;
    }

    public void setRaw(String raw) {
        this.raw = raw;
    }

    public String getCooked() {
        return cooked;
    }

    public void setCooked(String cooked) {
        this.cooked = cooked;
    }

    public DiscoursePost(Post jcommunePost) {
        this.id = (int)jcommunePost.getId();
        this.userId = (int)jcommunePost.getUserCreated().getId();
        this.topicId = (int)jcommunePost.getTopic().getId();
        this.createdAt = DiscourseMigration.jodaToJavaLocalDateTime(jcommunePost.getCreationDate());
        this.updatedAt = DiscourseMigration.jodaToJavaLocalDateTime(jcommunePost.getModificationDate());
        //this.postNumber = jcommunePost.getPostIndexInTopic() + 1;
        this.postNumber = jcommunePost.getRating();
        this.cooked = getCookedText(jcommunePost.getPostContent().replace("\u0000", "?"));
        this.raw = getRawText(jcommunePost.getPostContent().replace("\u0000", "?"));
    }

    public DiscoursePost(PostComment postComment) {
        this.id = (int)(DiscourseMigration.COMMENTS_ID_SHIFT + postComment.getId());
        this.userId = (int)postComment.getAuthor().getId();
        this.topicId = (int)postComment.getPost().getTopic().getId();
        this.createdAt = DiscourseMigration.jodaToJavaLocalDateTime(postComment.getCreationDate());
        this.updatedAt = DiscourseMigration.jodaToJavaLocalDateTime(postComment.getModificationDate());
        this.postNumber = postComment.getOwnerPost().getPostIndexInTopic() + 1;
        this.cooked = getCookedText(postComment.getBody());
        this.raw = getRawText(postComment.getBody());
    }

    private String getCookedText(String postContent) {
        ListItemsProcessor listsConverter = new ListItemsProcessor(postContent);
        TextProcessor stripBBCodesProcessor = BBProcessorFactory.getInstance()
                .createFromResource("kefirbb-discourse-cooked.xml");
        String cooked = stripBBCodesProcessor.process(listsConverter.getTextWithClosedTags().toString());
        return cooked;
    }

    private String getRawText(String postContent) {
        ListItemsProcessor listsConverter = new ListItemsProcessor(postContent);
        TextProcessor stripBBCodesProcessor = BBProcessorFactory.getInstance()
                .createFromResource("kefirbb-discourse-raw.xml");
        String raw = stripBBCodesProcessor.process(listsConverter.getTextWithClosedTags().toString());
        return raw;
    }
}
