package org.jtalks.jcommune.model.discourse;

import org.joda.time.DateTime;
import org.jtalks.common.model.entity.User;
import org.jtalks.jcommune.model.entity.*;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostsMigration {
    protected Connection mysqlConnection;
    protected Connection postgresqlConnection;

    private final String LOCALE = "ru";

    public PostsMigration(){}

    public PostsMigration(Connection mysql, Connection postgres) {
        mysqlConnection = mysql;
        postgresqlConnection = postgres;
    }

    public void startPostsMigration(int firstPostId, int postsPerRequest) {
        int lastPostId = getLastPostId();
        if (lastPostId == -1 || lastPostId < firstPostId) {
            throw new RuntimeException("Error when parsing command line args");
        }

        for(int i = firstPostId; i < lastPostId; i+= postsPerRequest ) {
            int to = i + postsPerRequest;
            if(to > lastPostId) {
                to = lastPostId;
            }
            String sql = "SELECT POST_ID FROM POST WHERE POST_ID >= ? AND POST_ID < ?";
            List<Integer> postIds = DiscourseMigration.getIds(sql, i, to);
            for(int j = 0; j < postIds.size(); j++) {
                try {
                    Post jcommunePost = getJcommunePost(postIds.get(j));
                    if (jcommunePost != null ) {
                        int postNumber = getPostNumberInTopic(jcommunePost.getTopic().getId(),
                                jcommunePost.getCreationDate());
                        addPost(jcommunePost, postNumber);
                        System.out.println("Post successfully migrated: id=" + String.valueOf(jcommunePost.getId()) );
                    }
                }
                catch(Exception e) {
                    throw new RuntimeException(String.format("Migration error (postId=%1$s): %2$s",
                            postIds.get(j), e.getMessage()));
                }
            }
        }
    }

    protected boolean addPost(Post jcommunePost, int postNumber) {
        DiscoursePost discoursePost = new DiscoursePost(jcommunePost);
        discoursePost.setPostNumber(postNumber);

        if(!insertToPosts(discoursePost, postgresqlConnection)) {
            return false;
        }

        if(!insertToPostSearchData(discoursePost, postgresqlConnection)) {
            return false;
        }

        return true;
    }

    private boolean insertToPosts(DiscoursePost discoursePost, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO posts(id, user_id, topic_id," +
                    " post_number, raw, cooked, created_at, updated_at, last_version_at)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
            ps.setInt(1, discoursePost.getId());
            ps.setInt(2, discoursePost.getUserId());
            ps.setInt(3, discoursePost.getTopicId());
            ps.setInt(4, discoursePost.getPostNumber());
            ps.setString(5, discoursePost.getRaw());
            ps.setString(6, discoursePost.getCooked());
            ps.setObject(7, discoursePost.getCreatedAt());
            ps.setObject(8, discoursePost.getUpdatedAt());
            ps.setObject(9, discoursePost.getCreatedAt());

            int i = ps.executeUpdate();
            if(i == 1) {
                return true;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error inserting to 'posts': " + e.getMessage());
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

    private int getLastPostId() {
        int maxId = -1;
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT MAX(POST_ID) FROM POST");
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                maxId = rs.getInt(1);
            }
        }
        catch(Exception e) {
            throw new RuntimeException("Can't get max post id in JCommune: " + e.getMessage());
        }
        return maxId;
    }

    private Post getJcommunePost(int id) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT POST.POST_ID, USER_CREATED, POST.TOPIC_ID, POST_CONTENT, POST_DATE, POST.MODIFICATION_DATE " +
                            "FROM POST " +
                            "INNER JOIN TOPIC " +
                            "ON POST.TOPIC_ID = TOPIC.TOPIC_ID " +
                            "WHERE TOPIC.TYPE != 'Code review' AND POST.POST_ID = ?");

            ps.setInt(1, id);

            ResultSet rs = ps.executeQuery();

            if(rs.next() == false) {
                System.out.println(String.format("No post with id=%1$s", id));
                return null;
            }

            JCUser author = new JCUser("", "", "");
            author.setId(rs.getLong("USER_CREATED"));

            Post jcommunePost = new Post(author, rs.getString("POST_CONTENT"));
            jcommunePost.setId(rs.getInt("POST_ID"));

            DateTime postCreationDate = DateTime.parse(rs.getString("POST_DATE"),
                    DiscourseMigration.MYSQL_DATETIME_FORMAT);
            Method setCreationDate = Post.class.getDeclaredMethod("setCreationDate", DateTime.class);
            setCreationDate.setAccessible(true);
            setCreationDate.invoke(jcommunePost, postCreationDate);

            String modificationDate = rs.getString("MODIFICATION_DATE");
            if(modificationDate == null) {
                modificationDate = rs.getString("POST_DATE");
            }
            DateTime postModificationDate = DateTime.parse(modificationDate,
                    DiscourseMigration.MYSQL_DATETIME_FORMAT);
            Method setModificationDate = Post.class.getDeclaredMethod("setModificationDate", DateTime.class);
            setModificationDate.setAccessible(true);
            setModificationDate.invoke(jcommunePost, postModificationDate);

            Topic topic = new Topic();
            topic.setId(rs.getInt("TOPIC_ID"));
            jcommunePost.setTopic(topic);
            return jcommunePost;
        }
        catch (Exception e)  {
            throw new RuntimeException("Can't create jcommunePost. " + e.getMessage());
        }
    }

    protected int getPostNumberInTopic(long topicId, DateTime postTime) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT COUNT(*) as number FROM POST " +
                    "WHERE TOPIC_ID = ? AND POST_DATE < ?");
            ps.setLong(1, topicId);
            ps.setString(2, postTime.toString(DiscourseMigration.MYSQL_DATETIME_FORMAT));

            ResultSet rs = ps.executeQuery();

            if(rs.next() == false) {
                System.out.println("Can't get post number: empty result set");
                return -1;
            }
            return rs.getInt("number");
        }
        catch (Exception e) {
            System.out.println("Can't get post number: " + e.getMessage());
            return -1;
        }
    }
}
