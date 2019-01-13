/**
 * Copyright (C) 2011  JTalks.org Team
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.jtalks.jcommune.model.discourse;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jtalks.common.model.entity.User;
import org.jtalks.jcommune.model.entity.JCUser;
import org.jtalks.jcommune.model.entity.UserContact;
import org.jtalks.jcommune.model.entity.UserContactType;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Data migration for discourse engine.
 *
 * @author Artem Reznyk
 */
public class DiscourseMigration {

    private Connection postgresqlConnection = null;
    private Connection mysqlConnection = null;
    private static final DateTimeFormatter MYSQL_DATETIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");
    public static final int WEBSITE_CONTACT_ID = 17;
    private static final int TRUST_LEVEL_BASIC_ID = 1;

    public static void main(String[] args) {
        DiscourseMigration migration = new DiscourseMigration();

        int start, count;
        try {
            start = Integer.parseInt(System.getProperty("start"));
            count = Integer.parseInt(System.getProperty("count"));
        }
        catch (Exception e) {
            throw new RuntimeException("Error when parsing command line args: " + e.getMessage());
        }

        int maxExistingUserId = migration.getLastUserId();
        if (maxExistingUserId == -1 || maxExistingUserId < start) {
            throw new RuntimeException("Error when parsing command line args");
        }

        for(int i = start; i < maxExistingUserId; i+= count ) {
            int to = i + count;
            List<Integer> userIds = migration.getUsersIds(i, to);
            for(int j = 0; j < userIds.size(); j++) {
                try {
                    JCUser jcommuneUser = migration.getJcommuneUser(userIds.get(j));
                    if (jcommuneUser != null) {
                        migration.addUser(jcommuneUser);
                    }
                }
                catch(Exception e) {
                    throw new RuntimeException("Migration error (jcUserId= " + String.valueOf(j) + "): " + e.getMessage());
                }
            }
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

    private List<Integer> getUsersIds(int from, int to) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement("SELECT ID FROM USERS " +
                    "WHERE ID >= ? AND ID < ?");
            ps.setInt(1, from);
            ps.setInt(2, to);
            ResultSet rs = ps.executeQuery();
            ArrayList<Integer> ids = new ArrayList<Integer>();
            while (rs.next())
            {
                ids.add(rs.getInt("ID"));
            };

            return ids;
        }
        catch (Exception e) {
            throw new RuntimeException("Can't get list of ids in JCommune " +
                    "(from=" + String.valueOf(from) + ", to=" + String.valueOf(from) + "): " + e.getMessage());
        }
    }

    public DiscourseMigration() {
        postgresqlConnection = ConnectionFactory.getPostgresqlConnection();
        mysqlConnection = ConnectionFactory.getMysqlConnection();
    }

    private JCUser getJcommuneUser(int id) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(
                    "SELECT ID, USERNAME, EMAIL, ENABLED, LAST_LOGIN, ROLE, BAN_REASON, FIRST_NAME, LAST_NAME, " +
                            "REGISTRATION_DATE, LOCATION, SEND_PM_NOTIFICATION," +
                            "VALUE " +
                            "FROM USERS LEFT JOIN JC_USER_DETAILS " +
                            "ON USERS.ID = JC_USER_DETAILS.USER_ID " +
                            "LEFT JOIN USER_CONTACT ON USERS.ID = USER_CONTACT.USER_ID " +
                            "WHERE ID = ? AND TYPE_ID = ? " +
                            "LIMIT 1");

            ps.setInt(1, id);
            ps.setInt(2, WEBSITE_CONTACT_ID);

            ResultSet rs = ps.executeQuery();
            rs.next();

            JCUser jcommuneUser = new JCUser(rs.getString("USERNAME"),
                    rs.getString("EMAIL"),
                    rs.getString("USERNAME"));

            //users
            jcommuneUser.setId(rs.getLong("ID"));
            jcommuneUser.setEmail(rs.getString("EMAIL"));
            jcommuneUser.setEnabled(rs.getBoolean("ENABLED"));
            DateTime lastLoginDate = DateTime.parse(rs.getString("LAST_LOGIN"), MYSQL_DATETIME_FORMAT);
            Method setLastLogin = User.class.getDeclaredMethod("setLastLogin", DateTime.class);
            Method setEncodedUsername = User.class.getDeclaredMethod("setEncodedUsername", String.class);
            setLastLogin.setAccessible(true);
            setEncodedUsername.setAccessible(true);
            setLastLogin.invoke(jcommuneUser, lastLoginDate);
            jcommuneUser.setRole(rs.getString("ROLE"));
            jcommuneUser.setBanReason(rs.getString("BAN_REASON"));
            jcommuneUser.setFirstName(rs.getString("FIRST_NAME"));
            jcommuneUser.setLastName(rs.getString("LAST_NAME"));

            //user_details
            DateTime regDate = DateTime.parse(rs.getString("REGISTRATION_DATE"), MYSQL_DATETIME_FORMAT);
            jcommuneUser.setRegistrationDate(regDate);
            jcommuneUser.setLocation(rs.getString("LOCATION"));
            jcommuneUser.setSendPmNotification(rs.getBoolean("SEND_PM_NOTIFICATION"));

            //user_contacts
            String contactValue = rs.getString("VALUE");
            if(contactValue != null && !contactValue.isEmpty()) {
                UserContactType websiteContactType = new UserContactType();
                websiteContactType.setId(WEBSITE_CONTACT_ID);
                UserContact websiteContact = new UserContact(contactValue, websiteContactType);
                jcommuneUser.addContact(websiteContact);
            }
            return jcommuneUser;
        }
        catch (Exception e)  {
            throw new RuntimeException("Can't create jcommuneUser. " + e.getMessage());
        }
    }

    public boolean addUser(JCUser jcommuneUser){
        DiscourseUser discourseUser = new DiscourseUser(jcommuneUser);

        if(!insertToUsers(discourseUser, postgresqlConnection)) {
            return false;
        }

        if(!insertToUserProfiles(discourseUser, postgresqlConnection)) {
            return false;
        }

        if(!insertToUserOptions(discourseUser, postgresqlConnection)) {
            return false;
        }

        return true;
    }

    private boolean insertToUsers(DiscourseUser discourseUser, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO users(ID, username, " +
                    "updated_at, email, active, last_seen_at, admin, previous_visit_at, first_seen_at, " +
                    "created_at, username_lower, trust_level, name)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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

            int i = ps.executeUpdate();
            if(i == 1) {
                return true;
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'users': " + ex.getMessage());
        }
        return false;
    }

    private boolean insertToUserProfiles(DiscourseUser discourseUser, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO user_profiles(user_id, " +
                    "location, website, bio_cooked_version)" +
                    " VALUES (?, ?, ?, ?)");
            ps.setInt(1, discourseUser.getId());
            ps.setString(2, discourseUser.getLocation());
            ps.setString(3, discourseUser.getWebsite());
            //same as for default user
            ps.setInt(4, 1);

            int i = ps.executeUpdate();
            if(i == 1) {
                return true;
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'user_profiles'", ex);
        }
        return false;
    }

    private boolean insertToUserOptions(DiscourseUser discourseUser, Connection connection) {
        try {
            PreparedStatement ps = connection.prepareStatement("INSERT INTO user_options(user_id, " +
                    "email_private_messages, email_digests, digest_after_minutes," +
                    "auto_track_topics_after_msecs, new_topic_duration_minutes, mailing_list_mode_frequency," +
                    "notification_level_when_replying)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            ps.setInt(1, discourseUser.getId());
            ps.setBoolean(2, discourseUser.getEmailPrivateMessages());

            //same as for default user
            ps.setBoolean(3, true);
            ps.setInt(4, 10800);
            ps.setInt(5, 240000);
            ps.setInt(6, 2880);
            ps.setInt(7, 0);
            ps.setInt(8, 2);

            int i = ps.executeUpdate();
            if(i == 1) {
                return true;
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Error inserting to 'user_options'. "  + ex.getMessage());
        }
        return false;
    }
}
