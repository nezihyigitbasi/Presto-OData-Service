package com.presto.odata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCTest {
    public static void main(String[] args) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:presto://localhost:8080/hive/information_schema", "test", null);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("show tables")) {
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
        //https://github.com/facebook/presto/issues/1283
        System.exit(0);
    }
}
