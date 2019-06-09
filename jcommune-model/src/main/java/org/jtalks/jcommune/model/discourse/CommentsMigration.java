package org.jtalks.jcommune.model.discourse;

import org.joda.time.DateTime;
import org.jtalks.jcommune.model.entity.JCUser;
import org.jtalks.jcommune.model.entity.Post;
import org.jtalks.jcommune.model.entity.Topic;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class CommentsMigration extends PostsMigration{

    public CommentsMigration(Connection mysql, Connection postgres) {
        super(mysql, postgres);
    }

    public void startCommentsMigration(int firstCommentId, int commentsPerRequest) {
        int lastCommentId = getLastCommentId();
        if (lastCommentId == -1 || lastCommentId < firstCommentId) {
            throw new RuntimeException("Error when parsing command line args");
        }

        for(int i = firstCommentId; i < lastCommentId; i+= commentsPerRequest ) {
            int to = i + commentsPerRequest;
            if(to > lastCommentId) {
                to = lastCommentId;
            }
            String sql = "SELECT ID FROM POST_COMMENT WHERE ID >= ? AND ID < ?";

            List<Integer> commentsIds = DiscourseMigration.getIds(sql, i, to);
            for(int j = 0; j < commentsIds.size(); j++) {
                try {
                    Post jcommunePost = getJcommunePostWithCommentData(commentsIds.get(j));
                    if (jcommunePost != null ) {
                        int postNumber = getPostNumberInTopic(jcommunePost.getTopic().getId(),
                                jcommunePost.getCreationDate());
                        addPost(jcommunePost, postNumber);
                        System.out.println("Comment successfully migrated: id=" + String.valueOf(jcommunePost.getId()) );
                    }
                }
                catch(Exception e) {
                    throw new RuntimeException(String.format("Migration error (postId=%1$s): %2$s",
                            commentsIds.get(j), e.getMessage()));
                }
            }
        }
    }

    private Post getJcommunePostWithCommentData(int commentId) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT POST_COMMENT.ID, POST.POST_ID, AUTHOR_ID, POST.TOPIC_ID, BODY, POST_COMMENT.CREATION_DATE, POST_COMMENT.MODIFICATION_DATE " +
                            "FROM POST " +
                            "INNER JOIN TOPIC " +
                            "ON POST.TOPIC_ID = TOPIC.TOPIC_ID " +
                            "INNER JOIN POST_COMMENT " +
                            "ON POST.POST_ID = POST_COMMENT.POST_ID " +
                            "WHERE TOPIC.TYPE != 'Code review' AND POST_COMMENT.ID = ?");

            ps.setInt(1, commentId);

            ResultSet rs = ps.executeQuery();

            if(rs.next() == false) {
                System.out.println(String.format("No comments with id=%1$s", commentId));
                return null;
            }

            JCUser author = new JCUser("", "", "");
            author.setId(rs.getLong("AUTHOR_ID"));

            Post jcommunePost = new Post(author, rs.getString("BODY"));
            jcommunePost.setId(DiscourseMigration.COMMENTS_ID_SHIFT + rs.getInt("ID"));

            DateTime postCreationDate = DateTime.parse(rs.getString("CREATION_DATE"),
                    DiscourseMigration.MYSQL_DATETIME_FORMAT);
            Method setCreationDate = Post.class.getDeclaredMethod("setCreationDate", DateTime.class);
            setCreationDate.setAccessible(true);
            setCreationDate.invoke(jcommunePost, postCreationDate);

            String modificationDate = rs.getString("MODIFICATION_DATE");
            if(modificationDate == null) {
                modificationDate = rs.getString("CREATION_DATE");
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
            throw new RuntimeException("Can't create post from comment. " + e.getMessage());
        }
    }

    private int getLastCommentId() {
        int maxId = -1;
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT MAX(ID) FROM POST_COMMENT");
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                maxId = rs.getInt(1);
            }
        }
        catch(Exception e) {
            throw new RuntimeException("Can't get max comment id in JCommune: " + e.getMessage());
        }
        return maxId;
    }
}
