package com.xipsoft.hotelapi.batch;

import com.xipsoft.hotelapi.batch.model.Room;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.concurrent.Callable;

public class RoomJob implements Callable<Room> {
    private static Logger logger = LoggerFactory.getLogger(RoomJob.class.getName());
    private DataSource dataSource;
    private Room room;
    private int hotelId;

    public RoomJob(DataSource dataSource, Room room, int hotelId) {
        this.dataSource = dataSource;
        this.room = room;
        this.hotelId = hotelId;
    }

    @Override
    public Room call() {
        try (Connection connection = dataSource.getConnection()) {
            logger.debug("Got connection :"+connection);
           try( PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO rooms (description,hotel_id) VALUES(?,?)")) {
               preparedStatement.setString(1, room.getDescription());
               preparedStatement.setInt(2, hotelId);
               preparedStatement.executeUpdate();
               try(Statement statement = connection.createStatement()) {
                   ResultSet resultSet = statement.executeQuery("SELECT LAST_INSERT_ID()");
                   if(resultSet.next()){
                       room.setId( resultSet.getInt(1));
                   }
                   resultSet.close();
               }
           }
        } catch (SQLException e) {
            logger.error("Error saving room",e);
        }
        logger.info("Finished room: "+room.getId());
        return room;
    }
}
