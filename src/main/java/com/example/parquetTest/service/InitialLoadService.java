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
        String editedDate = LocalDate.now().minusDays(4).toString(); // SYSDATE-1

        try (Connection conn = DuckDBUtil.getConnection()) {
            // Configure DuckDB for better performance - only use memory_limit and threads
            try (Statement configStmt = conn.createStatement()) {
                configStmt.execute("PRAGMA memory_limit='4GB'");
                configStmt.execute("PRAGMA threads=8");
            }

            for (Map.Entry<String, List<byte[]>> entry : files.entrySet()) {
                String folder = entry.getKey();
                ProcessInitialLoadService.FilterResult filterResult = new ProcessInitialLoadService.FilterResult(folder, editedDate);

                // Create the JSON directory if it doesn't exist
                new File("Json_InitialLoad").mkdirs();

                // Generate the output file path
                String jsonFilePath = "Json_InitialLoad/" + folder + "-" + LocalDate.parse(editedDate).format(
                        java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")) + ".json";

                // Delete the JSON file if it exists
                new File(jsonFilePath).delete();

                // Direct parquet to JSON export for this folder
                // Process all parquet files together for maximum speed
                // 1. Save all parquet bytes to temporary files
                List<File> tempParquetFiles = new ArrayList<>();
                for (byte[] parquetBytes : entry.getValue()) {
                    tempParquetFiles.add(saveTempParquetFile(parquetBytes));
                }

                try (Statement stmt = conn.createStatement()) {
                    // For smaller number of files, use direct UNION ALL
                    if (tempParquetFiles.size() <= 10) {
                        // Build the query to union all parquet files
                        StringBuilder unionQuery = new StringBuilder();
                        unionQuery.append("COPY (");

                        for (int i = 0; i < tempParquetFiles.size(); i++) {
                            if (i > 0) {
                                unionQuery.append(" UNION ALL ");
                            }
                            unionQuery.append("SELECT * FROM read_parquet('").append(tempParquetFiles.get(i).getAbsolutePath()).append("')");
                        }

                        unionQuery.append(") TO '").append(jsonFilePath).append("' (FORMAT JSON, ARRAY true)");

                        // Execute the export with a single SQL statement
                        stmt.execute(unionQuery.toString());

                        // Count the number of rows in the exports
                        StringBuilder countQuery = new StringBuilder();
                        countQuery.append("SELECT COUNT(*) FROM (");

                        for (int i = 0; i < tempParquetFiles.size(); i++) {
                            if (i > 0) {
                                countQuery.append(" UNION ALL ");
                            }
                            countQuery.append("SELECT * FROM read_parquet('").append(tempParquetFiles.get(i).getAbsolutePath()).append("')");
                        }

                        countQuery.append(")");

                        // Get the accurate count
                        try (ResultSet rs = stmt.executeQuery(countQuery.toString())) {
                            if (rs.next()) {
                                int count = rs.getInt(1);
                                // Use the accurate count from the SQL query
                                filterResult.files.clear();
                                filterResult.totalFilteredRows = 0;
                                filterResult.addFile(folder, count);
                            }
                        }
                    } else {
                        // For a large number of files, create a temporary table and bulk insert
                        // First file defines schema
                        String tempTable = "temp_combined_" + UUID.randomUUID().toString().replace("-", "_");
                        stmt.execute(String.format("CREATE TEMP TABLE %s AS SELECT * FROM read_parquet('%s');",
                                tempTable, tempParquetFiles.get(0).getAbsolutePath()));

                        // Insert data from all other files
                        for (int i = 1; i < tempParquetFiles.size(); i++) {
                            stmt.execute(String.format("INSERT INTO %s SELECT * FROM read_parquet('%s');",
                                    tempTable, tempParquetFiles.get(i).getAbsolutePath()));
                        }

                        // Export to JSON
                        stmt.execute(String.format("COPY (SELECT * FROM %s) TO '%s' (FORMAT JSON, ARRAY true);",
                                tempTable, jsonFilePath));

                        // Count
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempTable)) {
                            if (rs.next()) {
                                int count = rs.getInt(1);
                                filterResult.files.clear();
                                filterResult.totalFilteredRows = 0;
                                filterResult.addFile(folder, count);
                            }
                        }

                        // Drop temp table
                        stmt.execute("DROP TABLE " + tempTable);
                    }
                } catch (Exception e) {
                    // If the UNION ALL approach fails, fall back to processing files individually
                    System.err.println("UNION ALL approach failed for " + folder + ": " + e.getMessage());

                    try {
                        // Delete failed JSON file
                        new File(jsonFilePath).delete();

                        // Process each file individually
                        int totalCount = 0;
                        boolean first = true;
                        for (File tempFile : tempParquetFiles) {
                            try (Statement stmt = conn.createStatement()) {
                                String mode = first ? "w" : "a";
                                first = false;

                                // Count records in this file
                                int fileCount = 0;
                                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM read_parquet('" + tempFile.getAbsolutePath() + "')")) {
                                    if (rs.next()) {
                                        fileCount = rs.getInt(1);
                                        totalCount += fileCount;
                                    }
                                }

                                // Export to JSON (append mode after first file)
                                stmt.execute(String.format(
                                        "COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT JSON, ARRAY true, ARRAY_FORMAT '%s');",
                                        tempFile.getAbsolutePath(), jsonFilePath, mode));
                            }
                        }

                        // Set the count
                        filterResult.files.clear();
                        filterResult.totalFilteredRows = 0;
                        filterResult.addFile(folder, totalCount);
                    } catch (Exception fallbackException) {
                        System.err.println("Fallback approach also failed: " + fallbackException.getMessage());
                        e.printStackTrace();
                        fallbackException.printStackTrace();
                    }
                }

                // Clean up temp files
                for (File tempFile : tempParquetFiles) {
                    tempFile.delete();
                }

                // Add to results
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