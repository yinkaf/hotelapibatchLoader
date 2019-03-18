package com.xipsoft.hotelapi.batch;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xipsoft.hotelapi.batch.model.Hotel;
import com.xipsoft.hotelapi.batch.model.Room;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class BatchLoader {

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static Logger logger = LoggerFactory.getLogger(BatchLoader.class.getName());
    public static void main(String[] args) {
        if(args.length < 4){
            logger.info("Invalid Arguments: \n Parameters are [hotels filename] [database url] [database user] [password]");
            System.exit(-1);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        String filename = args[0];
        String dbUrl = args[1];
        String username = args[2];
        String password = args[3];

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(JDBC_DRIVER);
        config.setJdbcUrl(dbUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        HikariDataSource ds = new HikariDataSource(config);
        int hotelsRead = 0;
        int roomsRead = 0;
        int hotelsSaved = 0;
        int roomsSaved = 0;

        ObjectMapper mapper = new ObjectMapper();
        try( FileReader reader = new FileReader(filename)) {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createParser(reader);
            if(parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected an array");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                ObjectNode node = mapper.readTree(parser);
                Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
                Hotel hotel = new Hotel();
                hotel.setRooms(new ArrayList<>());
                while ( fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();

                    switch (field.getKey()){
                        case "name":
                            String hotelName = field.getValue().textValue();
                            hotel.setName(hotelName);
                            break;
                        case "description":
                            hotel.setDescription(field.getValue().textValue());
                            break;
                        case "cityCode":
                            hotel.setCityCode(field.getValue().textValue());
                            break;
                        case "rooms":
                            ArrayNode rooms = (ArrayNode) field.getValue();
                            for (int i = 0; i < rooms.size(); i++) {
                                JsonNode jsonNode = rooms.get(i);
                                hotel.getRooms().add(parseRoom(jsonNode.fields()));
                                roomsRead++;
                            }
                            break;
                            default:
                    }

                }
                hotelsRead++;
                Future<Hotel> future = executorService.submit(new HotelJob(ds, hotel));
                Hotel savedHotel = future.get();
                hotelsSaved++;
                logger.info("Saved Hotel: " + savedHotel);
                List<Future<Room>> futureRooms = new ArrayList<>();
                for(Room room : hotel.getRooms()) {
                   futureRooms.add( executorService.submit(new RoomJob(ds,room,hotel.getId())));
                }
                for(int i=0;i<futureRooms.size();i++){

                   Room room =  futureRooms.get(i).get();
                   logger.info("Saved Room: " + room);
                   roomsSaved++;
                }


            }
            logger.info("Read "+hotelsRead+" Hotels");
            logger.info("Read "+roomsRead+" Rooms");

            while (hotelsRead != hotelsSaved && roomsRead != roomsSaved) {
                logger.info("Saved "+hotelsSaved+" Hotels");
                logger.info("Saved "+roomsSaved + " Rooms");
                logger.info("Waiting for tasks to complete");
                Thread.sleep(500);
            }
            executorService.shutdown();
            logger.info("Saved "+hotelsSaved+" Hotels");
            logger.info("Saved "+roomsSaved + " Rooms");

        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(),e);
        }finally {
            ds.close();
        }
    }

    private static Room parseRoom(Iterator<Map.Entry<String, JsonNode>> roomFields) {
        Room room = new Room();
        while (roomFields.hasNext() ) {
            Map.Entry<String, JsonNode> field = roomFields.next();

            if ("description".equals(field.getKey())) {
                room.setDescription(field.getValue().textValue());
            }

        }
        return room;
    }


//    private static void writeHotel(Hotel hotel, HikariDataSource ds){
//        int hotelId = 0;
//        try (Connection connection = ds.getConnection()){
//
//            try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO hotels (hotel_name,description,city_code) VALUES(?,?,?)")) {
//                preparedStatement.setString(1, hotel.getName());
//                preparedStatement.setString(2, hotel.getDescription());
//                preparedStatement.setString(3, hotel.getCityCode());
//                preparedStatement.executeUpdate();
//
//
//               try(Statement statement = connection.createStatement()) {
//                   ResultSet resultSet = statement.executeQuery("SELECT LAST_INSERT_ID()");
//                   if(resultSet.next()){
//                      hotelId = resultSet.getInt(1);
//                   }
//                   resultSet.close();
//               }
//               if(hotelId != 0){
//                   for (Room room : hotel.getRooms()){
//                       writeRoom(room,hotelId,connection);
//                   }
//               }
//            }
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//    }

//    private static void writeRoom(Room room, int id, Connection connection) throws SQLException {
//        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO rooms (description,hotel_id) VALUES(?,?)");
//        preparedStatement.setString(1,room.getDescription());
//        preparedStatement.setInt(2,id);
//        preparedStatement.executeUpdate();
//    }
}
