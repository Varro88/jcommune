package org.jtalks.jcommune.model.discourse;

import org.jtalks.jcommune.model.entity.JCUser;
import org.jtalks.jcommune.model.entity.Post;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class BranchesMigration {

    private Connection mysqlConnection;
    private Connection postgresqlConnection;

    public BranchesMigration(Connection mysql, Connection postgres) {
        mysqlConnection = mysql;
        postgresqlConnection = postgres;
    }

    public void startBranchesMigration(int firstBranchIdId, int branchesPerRequest) {
        int lastBranchId = getLastBranchId();
        if (firstBranchIdId == -1 || lastBranchId < firstBranchIdId) {
            throw new RuntimeException("Error when parsing command line args");
        }

        for(int i = firstBranchIdId; i < lastBranchId; i+= branchesPerRequest ) {
            int to = i + branchesPerRequest;
            if(to > lastBranchId) {
                to = lastBranchId;
            }
            String sql = "SELECT BRANCH_ID FROM TOPIC WHERE BRANCH_ID >= ? AND BRANCH_ID < ?";
            List<Integer> branchesIds = DiscourseMigration.getIds(sql, i, to);
            for(int j = 0; j < branchesIds.size(); j++) {
                try {
                    MigrationBranch jcommuneBranch = getJcommuneBranch(branchesIds.get(j));
                    if (jcommuneBranch != null ) {
                        addBranch(jcommuneBranch);
                        System.out.println("Topic successfully migrated: id=" + String.valueOf(jcommuneBranch.getId()) );
                    }
                }
                catch(Exception e) {
                    throw new RuntimeException(String.format("Migration error (topicId=%1$s): %2$s",
                            branchesIds.get(j), e.getMessage()));
                }
            }
        }
    }

    private boolean addBranch(MigrationBranch jcommuneBranch) {
        DiscourseCategory discourseCategory = new DiscourseCategory(jcommuneBranch);

        if(!insertToCategories(discourseCategory, postgresqlConnection)) {
            return false;
        }
        return  true;
    }

    private boolean insertToCategories(DiscourseCategory discourseCategory, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO categories(id, name, topic_count, " +
                    "description, latest_post_id, latest_topic_id, position, created_at, updated_at, user_id, slug, " +
                    "name_lower) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
            ps.setInt(1, discourseCategory.getId());
            ps.setString(2, discourseCategory.getName());
            ps.setInt(3, discourseCategory.getTopicCount());
            ps.setString(4, discourseCategory.getDescription());
            ps.setInt(5, discourseCategory.getLatestPostId());
            ps.setInt(6, discourseCategory.getLatestTopicId());
            ps.setInt(7, discourseCategory.getPosition());
            ps.setObject(8, discourseCategory.getCreatedAt());
            ps.setObject(9, discourseCategory.getUpdatedAt());
            ps.setInt(10, -1);
            ps.setString(11, discourseCategory.getName().toLowerCase()
                    .replace(" ", "-")
                    .replace(".", ""));
            ps.setString(12, discourseCategory.getName().toLowerCase());
            int i = ps.executeUpdate();
            if(i == 1) {
                return true;
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Error inserting to 'categories': " + ex.getMessage());
        }
        return false;
    }

    private MigrationBranch getJcommuneBranch(int id) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT BRANCHES.BRANCH_ID, NAME, DESCRIPTION, POSITION, LAST_POST, POST.TOPIC_ID, COUNT(*) AS count " +
                            "FROM BRANCHES " +
                            "LEFT JOIN POST " +
                            "ON BRANCHES.LAST_POST = POST.POST_ID " +
                            "LEFT JOIN TOPIC " +
                            "ON TOPIC.BRANCH_ID = BRANCHES.BRANCH_ID " +
                            "WHERE BRANCHES.BRANCH_ID = ? " +
                            "GROUP BY BRANCHES.BRANCH_ID, NAME, DESCRIPTION, POSITION, LAST_POST, POST.TOPIC_ID");
            ps.setInt(1, id);

            ResultSet rs = ps.executeQuery();

            if(rs.next() == false) {
                System.out.println(String.format("No branches with id=%1$s", id));
                return null;
            }

            MigrationBranch branch = new MigrationBranch(rs.getString("NAME"), rs.getString("DESCRIPTION"));
            branch.setPosition(rs.getInt("POSITION"));
            branch.setId(rs.getInt("BRANCH_ID"));

            if(rs.getString("LAST_POST") == null) {
                System.out.println(String.format("No data in branch with id=%1$s", id));
                return null;
            }

            JCUser author = new JCUser("", "", "");
            Post lastPost = new Post(author, "");
            lastPost.setId(rs.getInt("LAST_POST"));
            branch.setLastPost(lastPost);
            branch.setLatestTopicId(rs.getInt("TOPIC_ID"));
            branch.setTopicsCount(rs.getInt("count"));
            return branch;
        }
        catch (Exception e) {
            throw new RuntimeException("Can't create jcommuneBranch. " + e.getMessage());
        }
    }

    private int getLastBranchId() {
        int maxId = -1;
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT MAX(BRANCH_ID) FROM BRANCHES");
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                maxId = rs.getInt(1);
            }
        }
        catch(Exception e) {
            throw new RuntimeException("Can't get max branch id in JCommune: " + e.getMessage());
        }
        return maxId;
    }
}
