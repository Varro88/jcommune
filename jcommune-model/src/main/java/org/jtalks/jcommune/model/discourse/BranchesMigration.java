package org.jtalks.jcommune.model.discourse;

import org.jtalks.jcommune.model.entity.JCUser;
import org.jtalks.jcommune.model.entity.Post;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class BranchesMigration {

    private final Connection mysqlConnection;
    private final Connection postgresqlConnection;

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

            System.out.println("First branch id in batch: " + i);
            List<MigrationBranch> branches = getJcommuneBranches(i, to);

            addBranches(branches);
        }
    }

    private List<MigrationBranch> getJcommuneBranches(int from, int to) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT BRANCHES.BRANCH_ID, NAME, DESCRIPTION, POSITION, LAST_POST, POST.TOPIC_ID, COUNT(*) AS count " +
                            "FROM BRANCHES " +
                            "LEFT JOIN POST " +
                            "ON BRANCHES.LAST_POST = POST.POST_ID " +
                            "LEFT JOIN TOPIC " +
                            "ON TOPIC.BRANCH_ID = BRANCHES.BRANCH_ID " +
                            "WHERE BRANCHES.BRANCH_ID >= ? AND BRANCHES.BRANCH_ID < ? " +
                            "GROUP BY BRANCHES.BRANCH_ID, NAME, DESCRIPTION, POSITION, LAST_POST, POST.TOPIC_ID");

            ps.setInt(1, from);
            ps.setInt(2, to);
            ResultSet rs = ps.executeQuery();

            List<MigrationBranch> branches = new ArrayList<>();

            while(rs.next()) {
                MigrationBranch branch = new MigrationBranch(rs.getString("NAME"), rs.getString("DESCRIPTION"));
                branch.setPosition(rs.getInt("POSITION"));
                branch.setId(rs.getInt("BRANCH_ID"));

                JCUser author = new JCUser("", "", "");
                Post lastPost = new Post(author, "");
                lastPost.setId(rs.getInt("LAST_POST"));
                branch.setLastPost(lastPost);
                branch.setLatestTopicId(rs.getInt("TOPIC_ID"));
                branch.setTopicsCount(rs.getInt("count"));

                branches.add(branch);
            }
            return branches;
        }
        catch (Exception ex) {
            throw new RuntimeException("Can't create jcommune branch.", ex);
        }
    }

    private void addBranches(List<MigrationBranch> branches) {
        List<DiscourseCategory> categories = DiscourseCategory.getCategories(branches);

        insertToCategories(categories, postgresqlConnection);
    }

    private void insertToCategories(List<DiscourseCategory> discourseCategories, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO categories(id, name, topic_count, " +
                    "description, latest_post_id, latest_topic_id, position, created_at, updated_at, user_id, slug, " +
                    "name_lower) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");

            for (DiscourseCategory category : discourseCategories) {
                ps.clearParameters();
                ps.setInt(1, category.getId());
                ps.setString(2, category.getName());
                ps.setInt(3, category.getTopicCount());
                ps.setString(4, category.getDescription());
                ps.setInt(5, category.getLatestPostId());
                ps.setInt(6, category.getLatestTopicId());
                ps.setInt(7, category.getPosition());
                ps.setObject(8, category.getCreatedAt());
                ps.setObject(9, category.getUpdatedAt());
                ps.setInt(10, -1);
                ps.setString(11, category.getName().toLowerCase()
                        .replace(" ", "-")
                        .replace(".", ""));
                ps.setString(12, category.getName().toLowerCase());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        catch (Exception ex) {
            throw new RuntimeException("Error inserting to 'categories'", ex);
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
