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
            connection.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS testshema1.users (" +
                    "id BIGINT PRIMARY KEY AUTO_INCREMENT, " +
                    "name VARCHAR(255), " +
                    "last_name VARCHAR(255), " +
                    "age INT)");
            connection.createStatement().executeUpdate("COMMIT");
        } catch (SQLException e) {
            e.printStackTrace();
            try (Connection connection = Util.getConnection()) {
                connection.createStatement().executeUpdate("ROLLBACK");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection()) {
            connection.createStatement().executeUpdate("DROP TABLE IF EXISTS testshema1.users");
            connection.createStatement().executeUpdate("COMMIT");
        } catch (SQLException e) {
            e.printStackTrace();
            try (Connection connection = Util.getConnection()) {
                connection.createStatement().executeUpdate("ROLLBACK");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = Util.getConnection();
             PreparedStatement pstm = connection.prepareStatement("INSERT INTO testshema1.users (name, last_name, age) VALUES (?, ?, ?)")) {
            pstm.setString(1, name);
            pstm.setString(2, lastName);
            pstm.setByte(3, age);
            pstm.executeUpdate();
            System.out.println(String.format("User с именем – %s добавлен в базу данных", name));
            connection.createStatement().executeUpdate("COMMIT");
        } catch (SQLException e) {
            e.printStackTrace();
            try (Connection connection = Util.getConnection()) {
                connection.createStatement().executeUpdate("ROLLBACK");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection();
             PreparedStatement pstm = connection.prepareStatement("DELETE FROM testshema1.users WHERE id = ?")) {
            pstm.setLong(1, id);
            pstm.executeUpdate();
            connection.createStatement().executeUpdate("COMMIT");
        } catch (SQLException e) {
            e.printStackTrace();
            try (Connection connection = Util.getConnection()) {
                connection.createStatement().executeUpdate("ROLLBACK");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    public List<User> getAllUsers() {
        try (Connection connection = Util.getConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery("select * from testshema1.users");
            List<User> result = new ArrayList<>();
            while (resultSet.next()) {
                User user = new User(resultSet.getString(2), resultSet.getString(3), resultSet.getByte(4));
                user.setId(resultSet.getLong(1));
                result.add(user);
            }
            connection.createStatement().executeUpdate("COMMIT");
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            try (Connection connection = Util.getConnection()) {
                connection.createStatement().executeUpdate("ROLLBACK");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
        return null;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection()) {
            connection.createStatement().executeUpdate("TRUNCATE TABLE testshema1.users");
            connection.createStatement().executeUpdate("COMMIT");
        } catch (SQLException e) {
            e.printStackTrace();
            try (Connection connection = Util.getConnection()) {
                connection.createStatement().executeUpdate("ROLLBACK");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}
