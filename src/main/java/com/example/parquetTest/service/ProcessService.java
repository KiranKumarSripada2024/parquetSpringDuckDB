package com.example.parquetTest.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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
    private static final String ZIP_FILE_NAME = JSON_DIR + ".zip";
    private final ObjectMapper objectMapper;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

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

        // Step 6: Zip the Json_filtered directory
        zipJsonFilteredDirectory();
    }

    private void saveJsonOutput(Map<String, FilterResult> filteredResults) {
        for (Map.Entry<String, FilterResult> entry : filteredResults.entrySet()) {
            String folderName = entry.getKey();
            FilterResult result = entry.getValue();
            String formattedDate = LocalDate.parse(result.editedDate).format(DATE_FORMATTER);
            File jsonOutputFile = new File(JSON_DIR, folderName + "-" + formattedDate + ".json");

            try (FileWriter writer = new FileWriter(jsonOutputFile)) {
                if (!result.data.isEmpty()) {
                    writer.write(objectMapper.writeValueAsString(result.data));
                } else {
                    writer.write("");
                }
                System.out.println("JSON saved: " + jsonOutputFile.getAbsolutePath());
            } catch (IOException e) {
                System.err.println("Error writing JSON file: " + jsonOutputFile.getName() + " - " + e.getMessage());
            }
        }
    }

    private void generateManifest(Map<String, FilterResult> filteredResults) {
        File manifestFile = new File(JSON_DIR, MANIFEST_FILE);
        try (FileWriter writer = new FileWriter(manifestFile)) {
            for (Map.Entry<String, FilterResult> entry : filteredResults.entrySet()) {
                FilterResult result = entry.getValue();
                String formattedDate = LocalDate.parse(result.editedDate).format(DATE_FORMATTER);
                writer.write(result.folderName + "|" + formattedDate + "|" + result.totalFilteredRows + "\n");
            }
            System.out.println("Manifest file saved: " + manifestFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Error writing manifest.txt: " + e.getMessage());
        }
    }

    private void zipJsonFilteredDirectory() {
        try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(ZIP_FILE_NAME))) {
            Path sourceDirPath = Paths.get(JSON_DIR);
            Files.walk(sourceDirPath).forEach(path -> {
                try {
                    String fileName = sourceDirPath.relativize(path).toString();
                    if (!fileName.isEmpty()) {
                        zipOut.putNextEntry(new ZipEntry(fileName));
                        if (!Files.isDirectory(path)) {
                            Files.copy(path, zipOut);
                        }
                        zipOut.closeEntry();
                    }
                } catch (IOException e) {
                    System.err.println("Error zipping file: " + path + " - " + e.getMessage());
                }
            });
            System.out.println("Zipped JSON directory: " + ZIP_FILE_NAME);
        } catch (IOException e) {
            System.err.println("Error creating ZIP file: " + e.getMessage());
        }
    }

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

    public static class FileDetail {
        public String file;
        public int recordCount;

        public FileDetail(String file, int recordCount) {
            this.file = file;
            this.recordCount = recordCount;
        }
    }
}
