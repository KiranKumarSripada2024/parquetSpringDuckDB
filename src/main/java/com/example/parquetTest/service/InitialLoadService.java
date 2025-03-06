package com.example.parquetTest.service;

import com.example.parquetTest.utils.DuckDBUtil;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;

@Service
public class InitialLoadService {

    public Map<String, ProcessInitialLoadService.FilterResult> filterParquetFiles(Map<String, List<byte[]>> files) {
        Map<String, ProcessInitialLoadService.FilterResult> folderResults = new HashMap<>();
        String editedDate = LocalDate.now().minusDays(1).toString(); // SYSDATE-1

        try (Connection conn = DuckDBUtil.getConnection()) {
            for (Map.Entry<String, List<byte[]>> entry : files.entrySet()) {
                String folder = entry.getKey();
                ProcessInitialLoadService.FilterResult filterResult = new ProcessInitialLoadService.FilterResult(folder, editedDate);

                // Setting "edited_date" for all except "view_events", which uses "event_time"
                String dateColumn = folder.equals("view_events") ? "event_time" : "edited_date";

                for (byte[] parquetBytes : entry.getValue()) {
                    // Step 1: Saving the Parquet file to a temporary location
                    File tempParquetFile = saveTempParquetFile(parquetBytes);

                    try (Statement stmt = conn.createStatement()) {
                        // Step 2: Loading the parquet files into DuckDB
                        String tempTable = "temp_parquet_" + UUID.randomUUID().toString().replace("-", "_");
                        stmt.execute(String.format("CREATE TEMP TABLE %s AS SELECT * FROM read_parquet('%s');",
                                tempTable, tempParquetFile.getAbsolutePath()));

                        // Step 3: Converting the datetime to YYYY-MM-DD
                        stmt.execute(String.format("ALTER TABLE %s ADD COLUMN filter_date STRING;", tempTable));
                        stmt.execute(String.format("UPDATE %s SET filter_date = strftime('%%Y-%%m-%%d', CAST(%s AS TIMESTAMP));",
                                tempTable, dateColumn));

                        // Step 5: Count rows only - don't load all data into memory
                        String countQuery = String.format("SELECT COUNT(*) AS cnt FROM %s WHERE filter_date <= '%s';",
                                tempTable, editedDate);
                        try (ResultSet rs = stmt.executeQuery(countQuery)) {
                            if (rs.next()) {
                                int rowCount = rs.getInt("cnt");
                                filterResult.addFile(tempParquetFile.getName(), rowCount);
                            }
                        }

                        // Step 6: Export the filtered data directly to a JSON file instead of loading into memory
                        String jsonFilePath = "Json_InitialLoad/" + folder + "-" + LocalDate.parse(editedDate).format(
                                java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")) + ".json";
                        new File("Json_InitialLoad").mkdirs(); // Ensure directory exists

                        stmt.execute(String.format(
                                "COPY (SELECT * EXCLUDE (filter_date) FROM %s WHERE filter_date <= '%s') TO '%s' (FORMAT JSON, ARRAY true);",
                                tempTable, editedDate, jsonFilePath));

                        // Step 7: Dropping the temporarily created table
                        stmt.execute("DROP TABLE " + tempTable);
                    }

                    tempParquetFile.delete();
                }

                // Step 8: Adding the result to map (without loading all data)
                folderResults.put(folder, filterResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return folderResults;
    }

    /**
     * Saves Parquet byte array to a temporary file.
     */
    private File saveTempParquetFile(byte[] parquetBytes) throws IOException {
        File tempFile = File.createTempFile("parquet_temp_", ".parquet");
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(parquetBytes);
        }
        return tempFile;
    }
}