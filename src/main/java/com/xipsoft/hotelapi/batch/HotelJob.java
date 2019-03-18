package com.xipsoft.hotelapi.batch;

import com.xipsoft.hotelapi.batch.model.Hotel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.concurrent.Callable;

public class HotelJob implements Callable<Hotel> {
    private static Logger logger = LoggerFactory.getLogger(HotelJob.class.getName());
    private DataSource dataSource;
    private Hotel hotel;

    public HotelJob(DataSource dataSource, Hotel hotel) {
        this.dataSource = dataSource;
        this.hotel = hotel;
    }

    @Override
    public Hotel call() throws Exception {
        int hotelId = 0;
        try (Connection connection = dataSource.getConnection()){
            logger.debug("Got connection :"+connection);
            try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO hotels (hotel_name,description,city_code) VALUES(?,?,?)")) {
                preparedStatement.setString(1, hotel.getName());
                preparedStatement.setString(2, hotel.getDescription());
                preparedStatement.setString(3, hotel.getCityCode());
                preparedStatement.executeUpdate();


                try(Statement statement = connection.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("SELECT LAST_INSERT_ID()");
                    if(resultSet.next()){
                        hotelId = resultSet.getInt(1);
                    }
                    resultSet.close();
                }
                hotel.setId(hotelId);
            }

        } catch (SQLException e) {
           logger.error("Error saving hotel",e);
        }
        logger.info("Finished hotel: "+hotel.getId());
        return hotel;
    }
}
