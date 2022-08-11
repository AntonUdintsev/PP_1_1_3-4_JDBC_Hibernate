package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() {
    }

    public void createUsersTable() {
        try (Connection connection = Util.getConnection()) {
            try {
                connection.setAutoCommit(false);
                connection.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS testshema1.users (" +
                        "id BIGINT PRIMARY KEY AUTO_INCREMENT, " +
                        "name VARCHAR(255), " +
                        "last_name VARCHAR(255), " +
                        "age INT)");
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection()) {
            try {
                connection.setAutoCommit(false);
                connection.createStatement().executeUpdate("DROP TABLE IF EXISTS testshema1.users");
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = Util.getConnection()) {
            try {
                connection.setAutoCommit(false);
                PreparedStatement pstm = connection.prepareStatement("INSERT INTO testshema1.users (name, last_name, age) VALUES (?, ?, ?)");
                pstm.setString(1, name);
                pstm.setString(2, lastName);
                pstm.setByte(3, age);
                pstm.executeUpdate();
                System.out.println(String.format("User с именем – %s добавлен в базу данных", name));
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement pstm = connection.prepareStatement("DELETE FROM testshema1.users WHERE id = ?")) {
                pstm.setLong(1, id);
                pstm.executeUpdate();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        try (Connection connection = Util.getConnection()) {
            try {
                connection.setAutoCommit(false);
                ResultSet resultSet = connection.createStatement().executeQuery("select * from testshema1.users");
                List<User> result = new ArrayList<>();
                while (resultSet.next()) {
                    User user = new User(resultSet.getString(2), resultSet.getString(3), resultSet.getByte(4));
                    user.setId(resultSet.getLong(1));
                    result.add(user);
                }
                connection.commit();
                return result;
            } catch (SQLException e) {
                connection.rollback();
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection()) {
            try {
                connection.setAutoCommit(false);
                connection.createStatement().executeUpdate("TRUNCATE TABLE testshema1.users");
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
