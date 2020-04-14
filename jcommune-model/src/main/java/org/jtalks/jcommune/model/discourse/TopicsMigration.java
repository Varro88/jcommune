package org.jtalks.jcommune.model.discourse;

import org.joda.time.DateTime;
import org.jtalks.jcommune.model.entity.Branch;
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
import java.util.stream.Collectors;

public class TopicsMigration {

    private final Connection mysqlConnection;
    private final Connection postgresqlConnection;

    public TopicsMigration(Connection mysql, Connection postgres) {
        mysqlConnection = mysql;
        postgresqlConnection = postgres;
    }

    public void startTopicsMigration(int firstTopicId, int topicsPerRequest) {
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

            System.out.println("First user id in batch: " + topicIds.get(0));
            List<Topic> topics = getJcommuneTopics(topicIds);
            addTopics(topics);
        }
    }

    private void addTopics(List<Topic> jcommuneTopics) {
        List<DiscourseTopic> discourseTopics = DiscourseTopic.getTopics(jcommuneTopics);

        insertToTopics(discourseTopics, postgresqlConnection);
    }

    private void insertToTopics(List<DiscourseTopic> discourseTopics, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO topics(id, title, last_posted_at, " +
                    "created_at, updated_at, views, posts_count, user_id, last_post_user_id, category_id, closed, " +
                    "pinned_at, bumped_at)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

            for(DiscourseTopic discourseTopic : discourseTopics) {
                ps.clearParameters();
                ps.setInt(1, discourseTopic.getId());
                ps.setString(2, discourseTopic.getTitle());
                ps.setObject(3, discourseTopic.getLastPostedAt());
                ps.setObject(4, discourseTopic.getCreatedAt());
                ps.setObject(5, discourseTopic.getUpdatedAt());
                ps.setInt(6, discourseTopic.getViews());
                ps.setInt(7, discourseTopic.getPostCount());
                ps.setInt(8, discourseTopic.getUserId());
                ps.setInt(9, discourseTopic.getLastPostUserId());
                ps.setInt(10, discourseTopic.getCategoryId());
                ps.setBoolean(11, discourseTopic.isClosed());
                if (discourseTopic.isPinned()) {
                    ps.setObject(12, discourseTopic.getCreatedAt());
                } else {
                    ps.setObject(12, null);
                }
                ps.setObject(13, discourseTopic.getLastPostedAt());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'posts'", ex);
        }
    }

    private List<Topic> getJcommuneTopics(List<Integer> ids) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT TOPIC.TOPIC_ID, TITLE, CREATION_DATE, TOPIC_STARTER, BRANCH_ID, VIEWS, STICKED, " +
                            "TOPIC.MODIFICATION_DATE, USER_CREATED as LAST_AUTHOR, COUNT(POST_ID) as POSTS_COUNT " +
                            "FROM TOPIC " +
                            "INNER JOIN POST " +
                            "ON TOPIC.TOPIC_ID = POST.TOPIC_ID " +
                            "WHERE TOPIC.TOPIC_ID IN (?) AND TOPIC.TYPE != 'Code review'" +
                            "GROUP BY POST.TOPIC_ID, USER_CREATED " +
                            "ORDER BY TOPIC.CREATION_DATE DESC " +
                            "LIMIT 1");

            ps.setString(1, ids.stream().map(String::valueOf).collect(Collectors.joining(",")));

            ResultSet rs = ps.executeQuery();

            List<Topic> topics = new ArrayList<>();
            while(rs.next()) {

                JCUser author = new JCUser("", "", "");
                author.setId(rs.getInt("TOPIC_STARTER"));

                Topic jcommuneTopic = new Topic(author, rs.getString("TITLE"));
                jcommuneTopic.setId(rs.getInt("TOPIC_ID"));

                DateTime topicCreationDate = DateTime.parse(rs.getString("CREATION_DATE"),
                        DiscourseMigration.MYSQL_DATETIME_FORMAT);
                Method setCreationDate = Topic.class.getDeclaredMethod("setCreationDate", DateTime.class);
                setCreationDate.setAccessible(true);
                setCreationDate.invoke(jcommuneTopic, topicCreationDate);

                Branch branch = new Branch("", "");
                branch.setId(rs.getInt("BRANCH_ID"));
                jcommuneTopic.setBranch(branch);

                jcommuneTopic.setViews(rs.getInt("VIEWS"));
                jcommuneTopic.setSticked(rs.getBoolean("STICKED"));

                for (int i = 0; i < rs.getInt("POSTS_COUNT") - 1; i++) {
                    jcommuneTopic.addPost(new Post(new JCUser("", "", ""), ""));
                }
                JCUser lastAuthor = new JCUser("", "", "");
                lastAuthor.setId(rs.getInt("LAST_AUTHOR"));
                jcommuneTopic.addPost(new Post(lastAuthor, ""));

                DateTime topicModificationDate = DateTime.parse(rs.getString("MODIFICATION_DATE"),
                        DiscourseMigration.MYSQL_DATETIME_FORMAT);
                Method setModificationDate = Topic.class.getDeclaredMethod("setModificationDate", DateTime.class);
                setModificationDate.setAccessible(true);
                setModificationDate.invoke(jcommuneTopic, topicModificationDate);

                topics.add(jcommuneTopic);
            }

            return topics;
        }
        catch (Exception ex) {
            throw new RuntimeException("Can't create jcommuneTopic.", ex);
        }
    }
}
