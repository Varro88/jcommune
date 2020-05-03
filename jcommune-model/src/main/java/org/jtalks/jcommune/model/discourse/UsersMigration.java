package org.jtalks.jcommune.model.discourse;

import org.joda.time.DateTime;
import org.jtalks.common.model.entity.User;
import org.jtalks.jcommune.model.entity.JCUser;
import org.jtalks.jcommune.model.entity.UserContact;
import org.jtalks.jcommune.model.entity.UserContactType;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UsersMigration {

    public static final int WEBSITE_CONTACT_ID = 17;
    private static final int TRUST_LEVEL_BASIC_ID = 1;

    private final Connection mysqlConnection;
    private final Connection postgresqlConnection;

    public UsersMigration(Connection mysql, Connection postgres) {
        mysqlConnection = mysql;
        postgresqlConnection = postgres;
    }

    public void startUsersMigration(int firstUserId, int usersPerRequest) {
        int maxExistingUserId = getLastUserId();
        if (maxExistingUserId == -1 || maxExistingUserId < firstUserId) {
            throw new RuntimeException("Error when parsing command line args");
        }

        for(int i = firstUserId; i < maxExistingUserId; i+= usersPerRequest ) {
            int to = i + usersPerRequest;
            if(to > maxExistingUserId) {
                to = maxExistingUserId;
            }

            System.out.println("First user id in batch: " + i);
            List<JCUser> users = getJcommuneUsers(i, to);

            addUser(users);
        }
    }

    private int getLastUserId() {
        int maxId = -1;
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT MAX(ID) FROM USERS");
            ResultSet rs = ps.executeQuery();
            if(rs.next()) {
                maxId = rs.getInt(1);
            }
        }
        catch(Exception e) {
            throw new RuntimeException("Can't get max user id in JCommune: " + e.getMessage());
        }
        return maxId;
    }

    private List<JCUser> getJcommuneUsers(int from, int to) {
        long currentUserId = -1;

        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT ID, USERNAME, EMAIL, ENABLED, LAST_LOGIN, ROLE, BAN_REASON, FIRST_NAME, LAST_NAME, " +
                            "POST_COUNT, REGISTRATION_DATE, LOCATION, SEND_PM_NOTIFICATION, VALUE " +
                            "FROM USERS " +
                            "LEFT JOIN JC_USER_DETAILS " +
                            "ON USERS.ID = JC_USER_DETAILS.USER_ID " +
                            "LEFT JOIN (SELECT MAX(CONTACT_ID), USER_ID, ANY_VALUE(VALUE) AS VALUE FROM USER_CONTACT " +
                            "WHERE USER_ID >= ? AND USER_ID < ? AND (TYPE_ID = ? OR TYPE_ID IS NULL) GROUP BY(USER_ID)) c " +
                            "ON USERS.ID = c.USER_ID " +
                            "WHERE ID >= ? AND ID < ? " +
                            "AND LAST_LOGIN IS NOT NULL AND REGISTRATION_DATE IS NOT NULL AND POST_COUNT IS NOT NULL;");

            ps.setInt(1, from);
            ps.setInt(2, to);
            ps.setInt(3, WEBSITE_CONTACT_ID);
            ps.setInt(4, from);
            ps.setInt(5, to);

            ResultSet rs = ps.executeQuery();

            List<JCUser> users = new ArrayList<>();
            while(rs.next()) {
                JCUser jcommuneUser = new JCUser(rs.getString("USERNAME"),
                        rs.getString("EMAIL"),
                        rs.getString("USERNAME"));

                //users
                jcommuneUser.setId(rs.getLong("ID"));
                currentUserId = jcommuneUser.getId();
                jcommuneUser.setEmail(rs.getString("EMAIL"));
                jcommuneUser.setEnabled(rs.getBoolean("ENABLED"));
                String last_login = rs.getString("LAST_LOGIN");
                DateTime lastLoginDate = null;
                if(last_login != null && last_login.isEmpty()) {
                    lastLoginDate = DateTime.parse(last_login,
                            DiscourseMigration.MYSQL_DATETIME_FORMAT);
                }
                Method setLastLogin = User.class.getDeclaredMethod("setLastLogin", DateTime.class);
                Method setEncodedUsername = User.class.getDeclaredMethod("setEncodedUsername", String.class);
                setLastLogin.setAccessible(true);
                setEncodedUsername.setAccessible(true);
                setLastLogin.invoke(jcommuneUser, lastLoginDate);
                jcommuneUser.setRole(rs.getString("ROLE"));
                jcommuneUser.setBanReason(rs.getString("BAN_REASON"));
                jcommuneUser.setFirstName(rs.getString("FIRST_NAME"));
                jcommuneUser.setLastName(rs.getString("LAST_NAME"));

                DateTime regDate = DateTime.parse(rs.getString("REGISTRATION_DATE"),
                            DiscourseMigration.MYSQL_DATETIME_FORMAT);

                jcommuneUser.setRegistrationDate(regDate);
                jcommuneUser.setLocation(rs.getString("LOCATION"));
                jcommuneUser.setSendPmNotification(rs.getBoolean("SEND_PM_NOTIFICATION"));
                jcommuneUser.setPostCount(rs.getInt("POST_COUNT"));

                //user_contacts
                String contactValue = rs.getString("VALUE");
                if(contactValue != null && !contactValue.isEmpty()) {
                    UserContactType websiteContactType = new UserContactType();
                    websiteContactType.setId(WEBSITE_CONTACT_ID);
                    UserContact websiteContact = new UserContact(contactValue, websiteContactType);
                    jcommuneUser.addContact(websiteContact);
                }
                users.add(jcommuneUser);
            }
            return users;
        }
        catch (Exception e)  {
            throw new RuntimeException("Can't create jcommuneUser id=" + currentUserId + e.getMessage(), e);
        }
    }

    public void addUser(List<JCUser> jcommuneUsers){
        List<DiscourseUser> users = DiscourseUser.getUsers(jcommuneUsers);

        insertToUsers(users, postgresqlConnection);
        insertToUserProfiles(users, postgresqlConnection);
        insertToUserOptions(users, postgresqlConnection);
        insertToUserStats(users, postgresqlConnection);
    }

    private void insertToUsers(List<DiscourseUser> discourseUsers, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO users(ID, username, " +
                    "updated_at, email, active, last_seen_at, admin, previous_visit_at, first_seen_at, " +
                    "created_at, username_lower, trust_level, name)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            for (DiscourseUser discourseUser : discourseUsers) {
                ps.clearParameters();
                ps.setInt(1, discourseUser.getId());
                ps.setString(2, discourseUser.getUsername());
                ps.setObject(3, discourseUser.getUpdatedAt());
                ps.setString(4, discourseUser.getEmail());
                ps.setBoolean(5, discourseUser.getActive());
                ps.setObject(6, discourseUser.getLastSeenAt());
                ps.setBoolean(7, discourseUser.getAdmin());
                ps.setObject(8, discourseUser.getPreviousVisitAt());
                ps.setObject(9, discourseUser.getFirstSeenAt());
                ps.setObject(10, discourseUser.getUpdatedAt());
                ps.setString(11, discourseUser.getUsername().toLowerCase());
                //trust level - basic
                ps.setInt(12, TRUST_LEVEL_BASIC_ID);
                ps.setString(13, discourseUser.getName());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'users'", ex);
        }
    }

    private void insertToUserProfiles(List<DiscourseUser> discourseUsers, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO user_profiles(user_id, " +
                    "location, website, bio_cooked_version)" +
                    " VALUES (?, ?, ?, ?)");
            for (DiscourseUser discourseUser : discourseUsers) {
                ps.clearParameters();

                ps.setInt(1, discourseUser.getId());
                ps.setString(2, discourseUser.getLocation());
                ps.setString(3, discourseUser.getWebsite());
                //same as for default user
                ps.setInt(4, 1);

                ps.addBatch();
            }

            ps.executeBatch();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'user_profiles'", ex);
        }
    }

    private void insertToUserOptions(List<DiscourseUser> discourseUsers, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO user_options(user_id, " +
                    "email_private_messages, email_digests, digest_after_minutes," +
                    "auto_track_topics_after_msecs, new_topic_duration_minutes, mailing_list_mode_frequency," +
                    "notification_level_when_replying)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

            for (DiscourseUser discourseUser : discourseUsers) {
                ps.clearParameters();

                ps.setInt(1, discourseUser.getId());
                ps.setBoolean(2, discourseUser.getEmailPrivateMessages());

                //same as for default user
                ps.setBoolean(3, false);
                ps.setInt(4, 10800);
                ps.setInt(5, 240000);
                ps.setInt(6, 2880);
                ps.setInt(7, 0);
                ps.setInt(8, 2);

                ps.addBatch();
            }

            ps.executeBatch();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'user_options'", ex);
        }
    }

    private void insertToUserStats(List<DiscourseUser> discourseUsers, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO user_stats(user_id, " +
                    "topic_reply_count, new_since)" +
                    " VALUES (?, ?, ?)");

            for (DiscourseUser discourseUser : discourseUsers) {
                ps.clearParameters();

                ps.setInt(1, discourseUser.getId());
                ps.setInt(2, discourseUser.getPostCount());
                ps.setObject(3, discourseUser.getFirstSeenAt());

                ps.addBatch();
            }

            ps.executeBatch();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'user_stats'", ex);
        }
    }
}
