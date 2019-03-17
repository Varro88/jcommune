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
import org.kefirsf.bb.BBProcessorFactory;
import org.kefirsf.bb.TextProcessor;

import java.sql.Connection;

/**
 * Data migration for discourse engine.
 *
 * @author Artem Reznyk
 */
public final class DiscourseMigration {

    public static final DateTimeFormatter MYSQL_DATETIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");
    private static Connection mysqlConnection;
    private static Connection postgresqlConnection;

    public static void main(String[] args) {
        mysqlConnection = ConnectionFactory.getMysqlConnection();
        postgresqlConnection = ConnectionFactory.getPostgresqlConnection();

        //startUsersMigration();
        startPostsMigration();
    }

    public static void startUsersMigration() {
        int firstUserId, usersPerRequest;
        try {
            firstUserId = Integer.parseInt(System.getProperty("firstUserId"));
            usersPerRequest = Integer.parseInt(System.getProperty("usersPerRequest"));
        }
        catch (Exception e) {
            throw new RuntimeException("Error when parsing command line args: " + e.getMessage());
        }
        UsersMigration usersMigration = new UsersMigration(mysqlConnection, postgresqlConnection);
        usersMigration.startUsersMigration(firstUserId, usersPerRequest);
    }

    public static void startPostsMigration() {
        int firstPostId, postsPerRequest;
        try {
            firstPostId = Integer.parseInt(System.getProperty("firstPostId"));
            postsPerRequest = Integer.parseInt(System.getProperty("postsPerRequest"));
        }
        catch (Exception e) {
            throw new RuntimeException("Error when parsing command line args: " + e.getMessage());
        }
        PostsMigration postsMigration = new PostsMigration(mysqlConnection, postgresqlConnection);
        postsMigration.startPostsMigration(firstPostId, postsPerRequest);
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
}
