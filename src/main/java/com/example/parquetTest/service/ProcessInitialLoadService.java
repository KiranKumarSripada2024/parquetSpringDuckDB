package com.example.parquetTest.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class ProcessInitialLoadService {
    private static final Logger logger = LoggerFactory.getLogger(ProcessInitialLoadService.class);

    @Autowired
    private DownloadService downloadService;

    @Autowired
    private ExtractionService extractionService;

    @Autowired
    private InitialLoadService initialLoadService;

    private static final String JSON_DIR = "Json_InitialLoad";
    private static final String ZIP_FILE_NAME = JSON_DIR + ".zip";
    private final ObjectMapper objectMapper;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public ProcessInitialLoadService() {
        this.objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    }

    public void process() throws Exception {
        // Step 1: Download ZIP file
        File zipFile = downloadService.downloadZip();

        // Step 2: Extract Parquet files into memory (byte arrays)
        Map<String, List<byte[]>> parquetFiles = extractionService.extractParquetFromZip(zipFile);

        // Step 3: Filter data using DuckDB (writing directly to files)
        Map<String, FilterResult> filteredResults = initialLoadService.filterParquetFiles(parquetFiles);

        // Step 4: Ensure Json_filtered directory exists
        File jsonDir = new File(JSON_DIR);
        if (!jsonDir.exists() && jsonDir.mkdirs()) {
            logger.info("Json_InitialLoad directory created: {}", jsonDir.getAbsolutePath());
        }

        // Step 5: Generate manifest.txt
        generateManifest(filteredResults);

        // Step 6: Zip the Json_filtered directory
        zipJsonFilteredDirectory();
    }

    private void generateManifest(Map<String, FilterResult> filteredResults) {
        Optional<String> optionalEditedDate = filteredResults.values().stream()
                .map(result -> result.editedDate)
                .filter(Objects::nonNull)
                .findFirst();

        if (optionalEditedDate.isPresent()) {
            String formattedDate = LocalDate.parse(optionalEditedDate.get()).format(DATE_FORMATTER);
            String manifestFileName = "manifest-" + formattedDate + ".txt";
            File manifestFile = new File(JSON_DIR, manifestFileName);

            try (FileWriter writer = new FileWriter(manifestFile)) {
                for (Map.Entry<String, FilterResult> entry : filteredResults.entrySet()) {
                    FilterResult result = entry.getValue();
                    writer.write(result.folderName + "|" + formattedDate + "|" + result.totalFilteredRows + "\n");
                }
                logger.info("Manifest file saved: {}", manifestFile.getAbsolutePath());
            } catch (IOException e) {
                logger.error("Error writing {}: {}", manifestFileName, e.getMessage());
            }
        } else {
            logger.error("Error: No valid edited_date found for manifest naming.");
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
                    logger.error("Error zipping file: {} - {}", path, e.getMessage());
                }
            });
            logger.info("Zipped JSON directory: " + ZIP_FILE_NAME);
        } catch (IOException e) {
            logger.error("Error creating ZIP file: {}", e.getMessage());
        }
    }

    public static class FilterResult {
        public String folderName;
        public String editedDate;
        public int totalFilteredRows;
        public List<FileDetail> files = new ArrayList<>();

        public FilterResult(String folderName, String editedDate) {
            this.folderName = folderName;
            this.editedDate = editedDate;
            this.totalFilteredRows = 0;
        }

        public void addData(Map<String, Object> row) {
            // Not storing data in memory anymore
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