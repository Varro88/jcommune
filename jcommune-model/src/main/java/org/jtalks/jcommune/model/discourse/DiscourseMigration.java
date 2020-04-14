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

        if(System.getProperty("firstUserId") != null && System.getProperty("usersPerRequest") != null) {
            int firstUserId = Integer.parseInt(System.getProperty("firstUserId"));
            int usersPerRequest = Integer.parseInt(System.getProperty("usersPerRequest"));
            startUsersMigration(firstUserId, usersPerRequest);
        }

        /*if(System.getProperty("firstCategoryId") != null && System.getProperty("categoriesPerRequest") != null) {
            int firstCategoryId = Integer.parseInt(System.getProperty("firstCategoryId"));
            int categoriesPerRequest = Integer.parseInt(System.getProperty("categoriesPerRequest"));
            startCategoriesMigration(firstCategoryId, categoriesPerRequest);
        }

        if(System.getProperty("firstTopicId") != null && System.getProperty("topicsPerRequest") != null) {
            int firstTopicId = Integer.parseInt(System.getProperty("firstTopicId"));
            int topicsPerRequest = Integer.parseInt(System.getProperty("topicsPerRequest"));
            startTopicsMigration(firstTopicId, topicsPerRequest);
        }

        if(System.getProperty("firstTopicWithContentId") != null && System.getProperty("topicsWithContentPerRequest") != null) {
            int firstTopicWithContentId = Integer.parseInt(System.getProperty("firstTopicWithContentId"));
            int topicsWithContentPerRequest = Integer.parseInt(System.getProperty("topicsWithContentPerRequest"));
            startTopicsContentMigration(firstTopicWithContentId, topicsWithContentPerRequest);
        }*/
    }

    public static void startUsersMigration(int firstUserId, int usersPerRequest) {
        UsersMigration usersMigration = new UsersMigration(mysqlConnection, postgresqlConnection);
        usersMigration.startUsersMigration(firstUserId, usersPerRequest);
    }

    public static void startCategoriesMigration(int firstCategoryId, int categoriesPerRequest) {
        BranchesMigration branchesMigration = new BranchesMigration(mysqlConnection, postgresqlConnection);
        branchesMigration.startBranchesMigration(firstCategoryId, categoriesPerRequest);
    }

    public static void startTopicsMigration(int firstTopicId, int topicsPerRequest) {
        TopicsMigration topicsMigration = new TopicsMigration(mysqlConnection, postgresqlConnection);
        topicsMigration.startTopicsMigration(firstTopicId, topicsPerRequest);
    }

    public static void startTopicsContentMigration(int firstTopicWithContentId, int topicsWithContentPerRequest) {
        TopicContentMigration topicContentMigration = new TopicContentMigration(mysqlConnection, postgresqlConnection);
        topicContentMigration.startTopicContentMigration(firstTopicWithContentId, topicsWithContentPerRequest);
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
