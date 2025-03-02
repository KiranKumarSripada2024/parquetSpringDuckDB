package com.example.parquetTest.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

@Service
public class ProcessService {

    @Autowired
    private DownloadService downloadService;

    @Autowired
    private ExtractionService extractionService;

    @Autowired
    private FilterService filterService;

    private static final String JSON_DIR = "Json_filtered";
    private static final String MANIFEST_FILE = "manifest.txt";
    private final ObjectMapper objectMapper;

    public ProcessService() {
        this.objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public void process() throws Exception {
        // Step 1: Download ZIP file
        File zipFile = downloadService.downloadZip();

        // Step 2: Extract Parquet files into memory (byte arrays)
        Map<String, List<byte[]>> parquetFiles = extractionService.extractParquetFromZip(zipFile);

        // Step 3: Filter data using DuckDB (directly from memory)
        Map<String, FilterResult> filteredResults = filterService.filterParquetFiles(parquetFiles);

        // Step 4: Ensure Json_filtered directory exists
        File jsonDir = new File(JSON_DIR);
        if (!jsonDir.exists() && jsonDir.mkdirs()) {
            System.out.println("Json_filtered directory created: " + jsonDir.getAbsolutePath());
        }

        // Step 5: Save JSON output and generate manifest.txt
        saveJsonOutput(filteredResults);
        generateManifest(filteredResults);
    }

    /**
     * Saves the JSON output for each folder.
     */
    private void saveJsonOutput(Map<String, FilterResult> filteredResults) {
        for (Map.Entry<String, FilterResult> entry : filteredResults.entrySet()) {
            String folderName = entry.getKey();
            FilterResult result = entry.getValue();
            File jsonOutputFile = new File(JSON_DIR, folderName + "-" + result.editedDate + ".json");

            try (FileWriter writer = new FileWriter(jsonOutputFile)) {
                if (result.totalFilteredRows > 0) {
                    // ✅ Structured JSON output using ObjectMapper
                    writer.write(objectMapper.writeValueAsString(new JsonOutput(result)));
                } else {
                    writer.write("{}");
                }
                System.out.println("JSON saved: " + jsonOutputFile.getAbsolutePath());
            } catch (IOException e) {
                System.err.println("Error writing JSON file: " + jsonOutputFile.getName() + " - " + e.getMessage());
            }
        }
    }

    /**
     * Generates the manifest.txt file.
     */
    private void generateManifest(Map<String, FilterResult> filteredResults) {
        File manifestFile = new File(JSON_DIR, MANIFEST_FILE);
        try (FileWriter writer = new FileWriter(manifestFile)) {
            for (Map.Entry<String, FilterResult> entry : filteredResults.entrySet()) {
                FilterResult result = entry.getValue();
                writer.write(result.folderName + "|" + result.editedDate + "|" + result.totalFilteredRows + "\n");
            }
            System.out.println("Manifest file saved: " + manifestFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error writing manifest.txt: " + e.getMessage());
        }
    }

    /**
     * Helper class to store filter results.
     */
    public static class FilterResult {
        public String folderName;
        public String editedDate;
        public int totalFilteredRows;
        public List<Map<String, Object>> data = new ArrayList<>();
        public List<FileDetail> files = new ArrayList<>();

        public FilterResult(String folderName, String editedDate) {
            this.folderName = folderName;
            this.editedDate = editedDate;
            this.totalFilteredRows = 0;
        }

        public void addData(Map<String, Object> row) {
            this.data.add(row);
        }

        public void addFile(String fileName, int recordCount) {
            this.files.add(new FileDetail(fileName, recordCount));
            this.totalFilteredRows += recordCount;
        }
    }

    /**
     * Helper class to store file details.
     */
    public static class FileDetail {
        public String file;
        public int recordCount;

        public FileDetail(String file, int recordCount) {
            this.file = file;
            this.recordCount = recordCount;
        }
    }

    /**
     * JSON Output format class (Ensures strict formatting)
     */
    public static class JsonOutput {
        public List<Map<String, Object>> data;
        public Controls controls;

        public JsonOutput(FilterResult result) {
            this.data = result.data;
            this.controls = new Controls(result);
        }
    }

    /**
     * Controls JSON structure
     */
    public static class Controls {
        public int total_filtered_rows;
        public String edited_date;
        public List<FileDetail> files;

        public Controls(FilterResult result) {
            this.total_filtered_rows = result.totalFilteredRows;
            this.edited_date = result.editedDate;
            this.files = result.files;
        }
    }
}