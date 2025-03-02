package com.example.parquetTest.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DuckDBUtil {

    private static final String DUCKDB_URL = "jdbc:duckdb:duckdb_data.db";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DUCKDB_URL);
    }

    public static void executeUpdate(String query) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

