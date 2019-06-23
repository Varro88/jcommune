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
import org.joda.time.DateTimeZone;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Data migration for discourse engine.
 *
 * @author Artem Reznyk
 */
public final class DiscourseMigration {

    public static final DateTimeFormatter MYSQL_DATETIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");
    private static Connection mysqlConnection;
    private static Connection postgresqlConnection;

    public final static int COMMENTS_ID_SHIFT = 1000000;

    public static void main(String[] args) {
        mysqlConnection = ConnectionFactory.getMysqlConnection();
        postgresqlConnection = ConnectionFactory.getPostgresqlConnection();

        startUsersMigration();
        startTopicsMigration();
        startTopicsContentMigration();
    }

    public static void startUsersMigration() {
        int firstUserId, usersPerRequest;
        try {
            firstUserId = Integer.parseInt(System.getProperty("firstUserId"));
            usersPerRequest = Integer.parseInt(System.getProperty("usersPerRequest"));
        }
        catch (Exception e) {
            System.out.println("Can't parse command line args for users: " + e.getMessage());
            return;
        }
        UsersMigration usersMigration = new UsersMigration(mysqlConnection, postgresqlConnection);
        usersMigration.startUsersMigration(firstUserId, usersPerRequest);
    }

    public static void startTopicsContentMigration() {
        int firstTopicId, topicsPerRequest;
        try {
            firstTopicId = Integer.parseInt(System.getProperty("firstTopicWithContentId"));
            topicsPerRequest = Integer.parseInt(System.getProperty("topicsWithContentPerRequest"));
        }
        catch (Exception e) {
            System.out.println("Can't parse command line args for comments: " + e.getMessage());
            return;
        }
        TopicContentMigration topicContentMigration = new TopicContentMigration(mysqlConnection, postgresqlConnection);
        topicContentMigration.startTopicContentMigration(firstTopicId, topicsPerRequest);
    }

    public static void startTopicsMigration() {
        int firstTopicId, topicsPerRequest;
        try {
            firstTopicId = Integer.parseInt(System.getProperty("firstTopicId"));
            topicsPerRequest = Integer.parseInt(System.getProperty("topicsPerRequest"));
        }
        catch (Exception e) {
            System.out.println("Can't parse command line args for topics: " + e.getMessage());
            return;
        }
        TopicsMigration topicsMigration = new TopicsMigration(mysqlConnection, postgresqlConnection);
        topicsMigration.startTopicsMigration(firstTopicId, topicsPerRequest);
    }

    public static java.time.LocalDateTime jodaToJavaLocalDateTime( DateTime dateTime ) {
        DateTime utcDateTime = dateTime.withZone(DateTimeZone.UTC);
        return java.time.LocalDateTime.of(
                utcDateTime.getYear(),
                utcDateTime.getMonthOfYear(),
                utcDateTime.getDayOfMonth(),
                utcDateTime.getHourOfDay(),
                utcDateTime.getMinuteOfHour(),
                utcDateTime.getSecondOfMinute());
    }

    public static List<Integer> getIds(String sql, int from, int to) {
        try {
            PreparedStatement ps = mysqlConnection.prepareStatement(sql);
            ps.setInt(1, from);
            ps.setInt(2, to);
            ResultSet rs = ps.executeQuery();
            List<Integer> ids = new ArrayList<Integer>();
            while (rs.next())
                ids.add(rs.getInt(1));
            return ids;
        }
        catch (Exception e) {
            throw new RuntimeException("Can't get list of ids " +
                    "(from=" + String.valueOf(from) + ", to=" + String.valueOf(to) + "): " + e.getMessage());
        }
    }

    public static int getLastTopicId() {
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
}
