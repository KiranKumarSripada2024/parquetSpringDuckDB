package com.example.parquetTest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ProcessInitialLoadServiceTest {

    @Mock
    private DownloadService downloadService;

    @Mock
    private ExtractionService extractionService;

    @Mock
    private InitialLoadService initialLoadService;

    @InjectMocks
    private ProcessInitialLoadService processInitialLoadService;

    private File mockZipFile;
    private Map<String, List<byte[]>> mockParquetFiles;
    private Map<String, ProcessInitialLoadService.FilterResult> mockFilteredResults;
    private String yesterdayDate;

    @BeforeEach
    void setUp() throws IOException {
        // Create a mock zip file
        mockZipFile = File.createTempFile("test_zip", ".zip");
        mockZipFile.deleteOnExit();

        // Setup yesterday's date
        yesterdayDate = LocalDate.now().minusDays(1).toString();

        // Setup mock parquet files map
        mockParquetFiles = new HashMap<>();
        mockParquetFiles.put("asset", Collections.singletonList(createMockParquetBytes("asset")));
        mockParquetFiles.put("view_events", Collections.singletonList(createMockParquetBytes("view_events")));

        // Setup mock filter results
        mockFilteredResults = new HashMap<>();
        mockFilteredResults.put("asset", createMockFilterResult("asset", 100));
        mockFilteredResults.put("view_events", createMockFilterResult("view_events", 200));
    }

    @Test
    void testProcess() throws Exception {
        // Arrange
        when(downloadService.downloadZip()).thenReturn(mockZipFile);
        when(extractionService.extractParquetFromZip(mockZipFile)).thenReturn(mockParquetFiles);
        when(initialLoadService.filterParquetFiles(mockParquetFiles)).thenReturn(mockFilteredResults);

        // Act
        processInitialLoadService.process();

        // Assert
        verify(downloadService, times(1)).downloadZip();
        verify(extractionService, times(1)).extractParquetFromZip(mockZipFile);
        verify(initialLoadService, times(1)).filterParquetFiles(mockParquetFiles);

        // Verify directory creation
        File jsonDir = new File("Json_InitialLoad");
        assertTrue(jsonDir.exists(), "JSON directory should be created");

        // Verify manifest file
        String formattedDate = LocalDate.parse(yesterdayDate).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File manifestFile = new File(jsonDir, "manifest-" + formattedDate + ".txt");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        // Verify zip file
        File zipFile = new File("Json_InitialLoad.zip");
        assertTrue(zipFile.exists(), "Zip file should be created");

        // Clean up
        manifestFile.delete();
        jsonDir.delete();
        zipFile.delete();
    }

    @Test
    void testGenerateManifest() throws Exception {
        // Create directory
        File jsonDir = new File("Json_InitialLoad");
        jsonDir.mkdirs();

        // Act
        ReflectionTestUtils.invokeMethod(processInitialLoadService, "generateManifest", mockFilteredResults);

        // Assert
        String formattedDate = LocalDate.parse(yesterdayDate).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File manifestFile = new File(jsonDir, "manifest-" + formattedDate + ".txt");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        // Clean up
        manifestFile.delete();
        jsonDir.delete();
    }

    @Test
    void testZipJsonFilteredDirectory() throws Exception {
        // Setup directory and files
        File jsonDir = new File("Json_InitialLoad");
        jsonDir.mkdirs();
        File testFile = new File(jsonDir, "test.txt");
        testFile.createNewFile();

        // Act
        ReflectionTestUtils.invokeMethod(processInitialLoadService, "zipJsonFilteredDirectory");

        // Assert
        File zipFile = new File("Json_InitialLoad.zip");
        assertTrue(zipFile.exists(), "Zip file should be created");

        // Clean up
        testFile.delete();
        jsonDir.delete();
        zipFile.delete();
    }

    // Helper methods
    private byte[] createMockParquetBytes(String folderName) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(baos);
        ZipEntry entry = new ZipEntry(folderName + "/test.parquet");
        zos.putNextEntry(entry);
        zos.write("Mock Parquet Data".getBytes());
        zos.closeEntry();
        zos.close();
        return baos.toByteArray();
    }

    private ProcessInitialLoadService.FilterResult createMockFilterResult(String folderName, int recordCount) {
        ProcessInitialLoadService.FilterResult result = new ProcessInitialLoadService.FilterResult(folderName, yesterdayDate);
        result.addFile("test.parquet", recordCount);
        return result;
    }
}