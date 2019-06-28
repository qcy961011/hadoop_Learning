package com.hive.conntrllerText;

import java.sql.*;

public class ConnText {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String diverName = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://192.168.88.195:10000/database";
        String user = "qcy96101111";
        String passWord = "123456";
        Class.forName(diverName);

        Connection connection = DriverManager.getConnection(url , user , passWord);
//        DriverManager.getConnection()
        Statement stmt = connection.createStatement();

        stmt.execute("use qiaochunyu");

        String sql = "select * from s1";

        ResultSet resultSet = stmt.executeQuery(sql);


        while (resultSet.next()){
            System.out.println(resultSet.getInt(1) + "\t" + resultSet.getString(2) + "\t" + resultSet.getString(3) + "\t" + resultSet.getInt(4));
        }


    }
}
