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

import com.mysql.jdbc.Driver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Postgresql connector.
 *
 * @author Artem Reznyk
 */
public class ConnectionFactory {
    public static final String POSTGRESQL_URL = "jdbc:postgresql://127.0.0.1:5432/discourse";
    public static final String POSTGRESQL_USER = "postgres";
    public static final String POSTGRESQL_PASS = "password";

    public static final String MYSQL_URL = "jdbc:mysql://127.0.0.1:3306/jcommune";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASS = "";
    /**
     * Get a connection to PostgresqlDatabase
     * @return Connection object
     */
    public static Connection getPostgresqlConnection()
    {
        try {
            DriverManager.registerDriver(new Driver());
            return DriverManager.getConnection(POSTGRESQL_URL, POSTGRESQL_USER, POSTGRESQL_PASS);
        } catch (SQLException ex) {
            throw new RuntimeException("Error connecting to the Postgresql database: " + ex.getMessage());
        }
    }

    /**
     * Get a connection to MysqlDatabase
     * @return Connection object
     */
    public static Connection getMysqlConnection()
    {
        try {
            DriverManager.registerDriver(new Driver());
            return DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASS);
        } catch (SQLException ex) {
            throw new RuntimeException("Error connecting to the MySQL database: " + ex.getMessage());
        }
    }


}