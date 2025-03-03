package com.example.parquetTest.service;

import com.example.parquetTest.utils.DuckDBUtil;
import org.springframework.stereotype.Service;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;

@Service
public class FilterService {

    public Map<String, ProcessService.FilterResult> filterParquetFiles(Map<String, List<byte[]>> files) {
        Map<String, ProcessService.FilterResult> folderResults = new HashMap<>();
        String editedDate = LocalDate.now().minusDays(1).toString(); // SYSDATE-1

        try (Connection conn = DuckDBUtil.getConnection();
             Statement stmt = conn.createStatement()) {

            for (Map.Entry<String, List<byte[]>> entry : files.entrySet()) {
                String folder = entry.getKey();
                ProcessService.FilterResult filterResult = new ProcessService.FilterResult(folder, editedDate);

                // Setting "edited_date" for all except "view_events", which uses "event_time"
                String dateColumn = folder.equals("view_events") ? "event_time" : "edited_date";

                for (byte[] parquetBytes : entry.getValue()) {
                    // Step 1: Saving the Parquet file to a temporary location
                    File tempParquetFile = saveTempParquetFile(parquetBytes);

                    // Step 2: Loading the parquet files into DuckDB
                    String tempTable = "temp_parquet_" + UUID.randomUUID().toString().replace("-", "_");
                    stmt.execute(String.format("CREATE TEMP TABLE %s AS SELECT * FROM read_parquet('%s');",
                            tempTable, tempParquetFile.getAbsolutePath()));

                    // Step 3: Converting the datetime to YYYY-MM-DD
                    stmt.execute(String.format("ALTER TABLE %s ADD COLUMN filter_date STRING;", tempTable));
                    stmt.execute(String.format("UPDATE %s SET filter_date = strftime('%%Y-%%m-%%d', CAST(%s AS TIMESTAMP));",
                            tempTable, dateColumn));

                    // Step 4: Filtering the Data using edited_date condition
                    String filterQuery = String.format("SELECT * FROM %s WHERE filter_date = '%s';", tempTable, editedDate);
                    try (ResultSet rs = stmt.executeQuery(filterQuery)) {
                        while (rs.next()) {
                            Map<String, Object> row = new HashMap<>();
                            int columnCount = rs.getMetaData().getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                row.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
                            }
                            filterResult.addData(row);
                        }
                    }

                    // Step 5: Counting the rows and adding the File Details
                    String countQuery = String.format("SELECT COUNT(*) AS cnt FROM %s WHERE filter_date = '%s';",
                            tempTable, editedDate);
                    try (ResultSet rs = stmt.executeQuery(countQuery)) {
                        if (rs.next()) {
                            int rowCount = rs.getInt("cnt");
                            filterResult.addFile(tempParquetFile.getName(), rowCount);
                        }
                    }

                    // Step 6: Dropping the temporarily created table
                    stmt.execute("DROP TABLE " + tempTable);
                    tempParquetFile.delete();
                }

                // Step 7: Adding the generated results to map
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
