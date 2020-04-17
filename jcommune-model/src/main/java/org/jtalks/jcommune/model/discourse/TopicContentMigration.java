package org.jtalks.jcommune.model.discourse;

import org.joda.time.DateTime;
import org.jtalks.jcommune.model.entity.JCUser;
import org.jtalks.jcommune.model.entity.Post;
import org.jtalks.jcommune.model.entity.Topic;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TopicContentMigration {
    protected Connection mysqlConnection;
    protected Connection postgresqlConnection;

    private final String LOCALE = "ru";

    public TopicContentMigration(Connection mysql, Connection postgres) {
        mysqlConnection = mysql;
        postgresqlConnection = postgres;
    }

    public void startTopicContentMigration(int firstTopicId, int topicsPerRequest) {
        int lastTopicId = DiscourseMigration.getLastTopicId();
        if (lastTopicId == -1 || lastTopicId < firstTopicId) {
            throw new RuntimeException("Error when parsing command line args");
        }

        for(int i = firstTopicId; i < lastTopicId; i+= topicsPerRequest ) {
            int to = i + topicsPerRequest;
            if(to > lastTopicId) {
                to = lastTopicId;
            }
            System.out.println("First topic with content id in batch: " + i);
            List<Post> posts = getAllPostsForTopics(i, to);

            addTopicsContent(posts);
        }
    }

    private void addTopicsContent(List<Post> posts) {
        List<DiscoursePost> discoursePosts = DiscoursePost.getPosts(posts);
        insertToPosts(discoursePosts, postgresqlConnection);
        insertToPostSearchData(discoursePosts, postgresqlConnection);
    }

    private List<Post> getAllPostsForTopics(int from, int to) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT 0 as is_comment, POST.POST_ID as entity_id, POST.POST_ID as post_id, " +
                    "TOPIC.TOPIC_ID as topic, USER_CREATED as author, POST_CONTENT as content, POST_DATE as created_at, " +
                    "POST.MODIFICATION_DATE as modified_at " +
                    "FROM POST " +
                    "INNER JOIN TOPIC " +
                    "ON POST.TOPIC_ID = TOPIC.TOPIC_ID " +
                    "WHERE TOPIC.TYPE != 'Code review' AND TOPIC.TOPIC_ID >= ? AND TOPIC.TOPIC_ID < ? " +
                    "UNION " +
                    "SELECT 1 as is_comment, POST_COMMENT.ID as entity_id, POST.POST_ID as post_id, " +
                    "TOPIC.TOPIC_ID as topic, AUTHOR_ID as author,  " +
                    "BODY as content, POST_COMMENT.CREATION_DATE as created_at, POST_COMMENT.CREATION_DATE as modified_at  " +
                    "FROM POST_COMMENT " +
                    "INNER JOIN POST " +
                    "ON POST.POST_ID = POST_COMMENT.POST_ID " +
                    "INNER JOIN TOPIC  " +
                    "ON POST.TOPIC_ID = TOPIC.TOPIC_ID " +
                    "WHERE TOPIC.TYPE != 'Code review' AND TOPIC.TOPIC_ID >= ? AND TOPIC.TOPIC_ID < ? " +
                    "ORDER BY topic, post_id, created_at");

            ps.setInt(1, from);
            ps.setInt(2, to);
            ps.setInt(3, from);
            ps.setInt(4, to);

            ResultSet rs = ps.executeQuery();

            List<Post> jcommunePosts = new ArrayList<>();
            int postNumber = 1;
            int previousTopicId = -1;

            while(rs.next()) {
                JCUser author = new JCUser("", "", "");
                author.setId(rs.getLong("author"));

                Post jcommunePost = new Post(author, rs.getString("content"));

                if(rs.getInt("is_comment") == 1) {
                    jcommunePost.setId(DiscourseMigration.COMMENTS_ID_SHIFT + rs.getInt("entity_id"));
                }
                else {
                    jcommunePost.setId(rs.getInt("entity_id"));
                }

                DateTime postCreationDate = DateTime.parse(rs.getString("created_at"),
                        DiscourseMigration.MYSQL_DATETIME_FORMAT);
                Method setCreationDate = Post.class.getDeclaredMethod("setCreationDate", DateTime.class);
                setCreationDate.setAccessible(true);
                setCreationDate.invoke(jcommunePost, postCreationDate);

                String modificationDate = rs.getString("modified_at");
                if(modificationDate == null) {
                    modificationDate = rs.getString("created_at");
                }
                DateTime postModificationDate = DateTime.parse(modificationDate,
                        DiscourseMigration.MYSQL_DATETIME_FORMAT);
                Method setModificationDate = Post.class.getDeclaredMethod("setModificationDate", DateTime.class);
                setModificationDate.setAccessible(true);
                setModificationDate.invoke(jcommunePost, postModificationDate);

                Topic topic = new Topic();
                topic.setId(rs.getInt("topic"));
                jcommunePost.setTopic(topic);

                if(previousTopicId == rs.getInt("topic")) {
                    postNumber++;
                }
                else {
                    previousTopicId = rs.getInt("topic");
                    postNumber = 1;
                }
                jcommunePost.setRating(postNumber);

                jcommunePosts.add(jcommunePost);
            }
            return jcommunePosts;
        }
        catch (Exception ex)  {
            throw new RuntimeException("Can't create posts for topic" , ex);
        }
    }

    private void insertToPosts(List<DiscoursePost> discoursePosts, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO posts(id, user_id, topic_id," +
                    " post_number, raw, cooked, created_at, updated_at, last_version_at)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");

            for(DiscoursePost discoursePost : discoursePosts) {
                ps.clearParameters();
                ps.setInt(1, discoursePost.getId());
                ps.setInt(2, discoursePost.getUserId());
                ps.setInt(3, discoursePost.getTopicId());
                ps.setInt(4, discoursePost.getPostNumber());
                ps.setString(5, discoursePost.getRaw());
                ps.setString(6, discoursePost.getCooked());
                ps.setObject(7, discoursePost.getCreatedAt());
                ps.setObject(8, discoursePost.getUpdatedAt());
                ps.setObject(9, discoursePost.getCreatedAt());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'posts'", ex);
        }
    }

    private void insertToPostSearchData(List<DiscoursePost> discoursePosts, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO post_search_data(post_id, " +
                    "search_data, raw_data, locale) VALUES (?, to_tsvector('russian', ?), ?, ?)");

            for(DiscoursePost discoursePost : discoursePosts) {
                ps.clearParameters();
                ps.setInt(1, discoursePost.getId());
                ps.setString(2, discoursePost.getRaw());
                ps.setString(3, discoursePost.getRaw());
                ps.setString(4, LOCALE);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        catch (Exception ex) {
            throw new RuntimeException("Error inserting to 'post_search_data'", ex);
        }
    }
}
