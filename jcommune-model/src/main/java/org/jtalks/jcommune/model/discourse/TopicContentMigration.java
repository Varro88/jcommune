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
            String sql = "SELECT TOPIC_ID FROM TOPIC WHERE TOPIC_ID >= ? AND TOPIC_ID < ?";
            List<Integer> topicIds = DiscourseMigration.getIds(sql, i, to);
            for(int j = 0; j < topicIds.size(); j++) {
                try {
                    List<Post> content = getAllPostsForTopic(topicIds.get(j));
                    if (content != null ) {
                        for (int k = 0; k < content.size(); k++) {
                            Post post = content.get(k);
                            addTopicContent(post, k+1);
                            System.out.println("Topic post successfully migrated: topic_id="
                                    + String.valueOf(topicIds.get(j)) + "post_id=" + post.getId() );
                        }
                    }
                }
                catch(Exception e) {
                    throw new RuntimeException(String.format("Migration error for topic content (topicId=%1$s): %2$s",
                            topicIds.get(j), e.getMessage()));
                }
            }
        }
    }

    private boolean addTopicContent(Post post, int postNumber) {
        DiscoursePost discoursePost = new DiscoursePost(post);
        discoursePost.setPostNumber(postNumber);

        if(!insertToPosts(discoursePost, postgresqlConnection)) {
            return false;
        }

        if(!insertToPostSearchData(discoursePost, postgresqlConnection)) {
            return false;
        }

        return true;
    }

    private List<Post> getAllPostsForTopic(int topicId) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT 0 as is_comment, POST.POST_ID as entity_id, POST.POST_ID as post_id, " +
                    "TOPIC.TOPIC_ID as topic, USER_CREATED as author, POST_CONTENT as content, POST_DATE as created_at, " +
                    "POST.MODIFICATION_DATE as modified_at " +
                    "FROM POST " +
                    "INNER JOIN TOPIC " +
                    "ON POST.TOPIC_ID = TOPIC.TOPIC_ID " +
                    "WHERE TOPIC.TYPE != 'Code review' AND TOPIC.TOPIC_ID = ? " +
                    "UNION " +
                    "SELECT 1 as is_comment, POST_COMMENT.ID as entity_id, POST.POST_ID as post_id, " +
                    "TOPIC.TOPIC_ID as topic, AUTHOR_ID as author,  " +
                    "BODY as content, POST_COMMENT.CREATION_DATE as created_at, POST_COMMENT.MODIFICATION_DATE as modified_at  " +
                    "FROM POST_COMMENT " +
                    "INNER JOIN POST " +
                    "ON POST.POST_ID = POST_COMMENT.POST_ID " +
                    "INNER JOIN TOPIC  " +
                    "ON POST.TOPIC_ID = TOPIC.TOPIC_ID " +
                    "WHERE TOPIC.TYPE != 'Code review' AND TOPIC.TOPIC_ID = ? " +
                    "ORDER BY post_id, created_at");

            ps.setInt(1, topicId);
            ps.setInt(2, topicId);
            ResultSet rs = ps.executeQuery();

            List<Post> jcommunePosts = new ArrayList<Post>();
            while(rs.next() != false) {
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

                jcommunePosts.add(jcommunePost);
            }
            return jcommunePosts;
        }
        catch (Exception e)  {
            throw new RuntimeException("Can't create posts for topic. " + e.getMessage());
        }
    }

    private boolean insertToPosts(DiscoursePost discoursePost, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO posts(user_id, topic_id," +
                    " post_number, raw, cooked, created_at, updated_at, last_version_at)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?);");
            //using auto ids
            //ps.setInt(1, discoursePost.getId());
            ps.setInt(1, discoursePost.getUserId());
            ps.setInt(2, discoursePost.getTopicId());
            ps.setInt(3, discoursePost.getPostNumber());
            ps.setString(4, discoursePost.getRaw());
            ps.setString(5, discoursePost.getCooked());
            ps.setObject(6, discoursePost.getCreatedAt());
            ps.setObject(7, discoursePost.getUpdatedAt());
            ps.setObject(8, discoursePost.getCreatedAt());

            int i = ps.executeUpdate();
            if(i == 1) {
                return true;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error inserting to 'posts' (id=" + discoursePost.getId() + "): " + e.getMessage());
        }
        return false;
    }

    private boolean insertToPostSearchData(DiscoursePost discoursePost, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO post_search_data(post_id, " +
                    "search_data, raw_data, locale) VALUES (?, to_tsvector('russian', ?), ?, ?)");

            ps.setInt(1, discoursePost.getId());
            ps.setString(2, discoursePost.getRaw());
            ps.setString(3, discoursePost.getRaw());
            ps.setString(4, LOCALE);

            int i = ps.executeUpdate();
            if(i == 1) {
                return true;
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error inserting to 'post_search_data': " + e.getMessage());
        }
        return false;
    }

}
