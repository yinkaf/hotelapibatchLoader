# hotelapibatchLoader

Hotel rest data upload

This command line application allows Hotels and associated Rooms to be loaded in a batch.
Hotels and Room data must be in a JSON file with the following structure.
```json
  [
  {
    "name": "Hotel1",
    "description": "Hotel 1 description",
    "cityCode": "lon",
    "rooms": [
      {
        "description": "Basic Room"
      },{
        "description": "Double room"
      },{
        "description": "Executive Room"
      }
    ]
  },

  {
    "name": "Hotel2",
    "description": "Hotel 2 description",
    "cityCode": "lon",
    "rooms": [
      {
        "description": "Basic Room"
      },{
        "description": "Double room"
      },{
        "description": "Executive Room"
      }
    ]
  }
  ]
  ```
  
  The file is streamed and each hotel data block is parsed.
  
  Hotel and Room data is converted to POJO objects.
  
  The application uses a ThreadPoolExecutor configured with 5 threads to execute WriteJobs for Hotel and Room.
  
  A database connection pool is created using [HikariCP](https://github.com/brettwooldridge/HikariCP).
  
  This application uses JDBC to insert data to the database.
  
  ## Running the application
  
  This is a maven project.
  
  Build the app with command.
  
    mvn clean install
   
  execute the app by command
    
    java -jar target/hotelapi-batch-1.0-SNAPSHOT-jar-with-dependencies.jar [hotels JSON file] [database url] [database user] [password]
  
