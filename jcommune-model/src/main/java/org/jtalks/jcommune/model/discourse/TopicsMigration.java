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

public class TopicsMigration {

    private Connection mysqlConnection;
    private Connection postgresqlConnection;

    public TopicsMigration(Connection mysql, Connection postgres) {
        mysqlConnection = mysql;
        postgresqlConnection = postgres;
    }

    public void startTopicsMigration(int firstTopicId, int topicsPerRequest) {
        int lastTopicId = getLastTopicId();
        if (lastTopicId == -1 || lastTopicId < firstTopicId) {
            throw new RuntimeException("Error when parsing command line args");
        }

        for(int i = firstTopicId; i < lastTopicId; i+= topicsPerRequest ) {
            int to = i + topicsPerRequest;
            if(to > lastTopicId) {
                to = lastTopicId;
            }
            List<Integer> topicIds = getTopicsIds(i, to);
            for(int j = 0; j < topicIds.size(); j++) {
                try {
                    Topic jcommuneTopic = getJcommuneTopic(topicIds.get(j));
                    if (jcommuneTopic != null ) {
                        addTopic(jcommuneTopic);
                        System.out.println("Topic successfully migrated: id=" + String.valueOf(jcommuneTopic.getId()) );
                    }
                }
                catch(Exception e) {
                    throw new RuntimeException(String.format("Migration error (topicId=%1$s): %2$s",
                            topicIds.get(j), e.getMessage()));
                }
            }
        }
    }

    private boolean addTopic(Topic jcommuneTopic) {
        DiscourseTopic discourseTopic = new DiscourseTopic(jcommuneTopic);

        if(!insertToTopics(discourseTopic, postgresqlConnection)) {
            return false;
        }

        return true;
    }

    private boolean insertToTopics(DiscourseTopic discourseTopic, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO topics(id, title, last_posted_at, " +
                    "created_at, updated_at, views, posts_count, user_id, last_post_user_id, category_id, closed, " +
                    "pinned_at, bumped_at)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
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
            if(discourseTopic.isPinned()) {
                ps.setObject(12, discourseTopic.getCreatedAt());
            }
            else {
                ps.setObject(12, null);
            }
            ps.setObject(13, discourseTopic.getLastPostedAt());

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

    private int getLastTopicId() {
        int maxId = -1;
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT MAX(TOPIC_ID) FROM TOPIC");
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                maxId = rs.getInt(1);
            }
        }
        catch(Exception e) {
            throw new RuntimeException("Can't get max topic id in JCommune: " + e.getMessage());
        }
        return maxId;
    }

    private List<Integer> getTopicsIds(int from, int to) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT TOPIC_ID FROM TOPIC " +
                    "WHERE TOPIC_ID >= ? AND TOPIC_ID < ?");
            ps.setInt(1, from);
            ps.setInt(2, to);
            ResultSet rs = ps.executeQuery();
            ArrayList<Integer> ids = new ArrayList<Integer>();
            while (rs.next())
            {
                ids.add(rs.getInt("TOPIC_ID"));
            };

            return ids;
        }
        catch (Exception e) {
            throw new RuntimeException("Can't get list of ids for topics in JCommune " +
                    "(from=" + String.valueOf(from) + ", to=" + String.valueOf(to) + "): " + e.getMessage());
        }
    }

    private Topic getJcommuneTopic(int id) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT TOPIC.TOPIC_ID, TITLE, CREATION_DATE, TOPIC_STARTER, BRANCH_ID, VIEWS, STICKED, " +
                            "TOPIC.MODIFICATION_DATE, USER_CREATED as LAST_AUTHOR, COUNT(POST_ID) as POSTS_COUNT " +
                            "FROM TOPIC " +
                            "INNER JOIN POST " +
                            "ON TOPIC.TOPIC_ID = POST.TOPIC_ID " +
                            "WHERE TOPIC.TOPIC_ID = ? " +
                            "GROUP BY POST.TOPIC_ID, USER_CREATED " +
                            "ORDER BY TOPIC.CREATION_DATE DESC " +
                            "LIMIT 1");

            ps.setInt(1, id);

            ResultSet rs = ps.executeQuery();

            if (rs.next() == false) {
                System.out.println(String.format("No topic with id=%1$s", id));
                return null;
            }

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

            DateTime topicModificationDate = DateTime.parse(rs.getString("MODIFICATION_DATE"),
                    DiscourseMigration.MYSQL_DATETIME_FORMAT);
            Method setModificationDate = Topic.class.getDeclaredMethod("setModificationDate", DateTime.class);
            setModificationDate.setAccessible(true);
            setModificationDate.invoke(jcommuneTopic, topicModificationDate);

            for (int i = 0; i < rs.getInt("POSTS_COUNT") - 1; i++) {
                jcommuneTopic.addPost(new Post(new JCUser("", "", ""), ""));
            }
            JCUser lastAuthor = new JCUser("", "", "");
            lastAuthor.setId(rs.getInt("LAST_AUTHOR"));
            jcommuneTopic.addPost(new Post(lastAuthor, ""));

            return jcommuneTopic;
        }
        catch (Exception e) {
            throw new RuntimeException("Can't create jcommuneTopic. " + e.getMessage());
        }
    }
}
