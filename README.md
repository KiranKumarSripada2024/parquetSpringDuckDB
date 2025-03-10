# Parquet File Processing with Spring Boot and DuckDB

## VM OPTIONS
-Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200

## API CALLING

### For Initial Load:-
http://localhost:8080/api/parquet/initialLoad

### For Processing Delta Changes:-
http://localhost:8080/api/parquet/process

## Overview  
This Spring Boot project processes Parquet files from a ZIP archive, filters them using DuckDB, and generates structured JSON output along with a `manifest.txt` file.

## Features  
- Downloads a ZIP file containing Parquet files.  
- Extracts Parquet files into memory.  
- Filters data using DuckDB based on `edited_date` (and `event_time` for `view_events`).  
- Saves filtered data in structured JSON format.  
- Generates a `manifest.txt` file with folder-wise record counts.  
- Handles errors and logs them in `error.json`.

## Project Structure  
```
/src/main/java/com/example/parquetTest
|--config/AppConfig.java 
├── controller/ParquetController.java  
├── service/  
│   ├── DownloadService.java  
│   ├── ExtractionService.java  
│   ├── FilterService.java  
│   ├── ProcessService.java  
├── util/DuckDBUtil.java  
└── ParquetTestApplication.java  
```

## Configuration  
Update `application.properties` with credentials:  
```properties
app.username=your_username  
app.password=your_password  
```

## Run the Application  
Use Maven to build and run the project:  
```sh
mvn spring-boot:run
```

## Expected Output  
- JSON files in `Json_filtered/` (e.g., `asset-2025-02-28.json`)  
- `manifest.txt` with format:  
  ```
  asset|2025-02-28|209
  view_events|2025-02-28|359
  ```

## Dependencies  
- Spring Boot  
- DuckDB  
- Apache Parquet  
- Jackson for JSON processing  
