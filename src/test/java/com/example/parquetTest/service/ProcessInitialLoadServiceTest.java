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
        mockZipFile = File.createTempFile("test_zip", ".zip");
        mockZipFile.deleteOnExit();

        yesterdayDate = LocalDate.now().minusDays(1).toString();

        mockParquetFiles = new HashMap<>();
        mockParquetFiles.put("asset", Collections.singletonList(createMockParquetBytes("asset")));
        mockParquetFiles.put("view_events", Collections.singletonList(createMockParquetBytes("view_events")));

        mockFilteredResults = new HashMap<>();
        mockFilteredResults.put("asset", createMockFilterResult("asset", 100));
        mockFilteredResults.put("view_events", createMockFilterResult("view_events", 200));
    }

    @Test
    void testProcess() throws Exception {
        when(downloadService.downloadZip()).thenReturn(mockZipFile);
        when(extractionService.extractParquetFromZip(mockZipFile)).thenReturn(mockParquetFiles);
        when(initialLoadService.filterParquetFiles(mockParquetFiles)).thenReturn(mockFilteredResults);

        processInitialLoadService.process();

        verify(downloadService, times(1)).downloadZip();
        verify(extractionService, times(1)).extractParquetFromZip(mockZipFile);
        verify(initialLoadService, times(1)).filterParquetFiles(mockParquetFiles);

        File jsonDir = new File("Json_InitialLoad");
        assertTrue(jsonDir.exists(), "JSON directory should be created");

        String formattedDate = LocalDate.parse(yesterdayDate).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File manifestFile = new File(jsonDir, "manifest-" + formattedDate + ".txt");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        File zipFile = new File("Json_InitialLoad.zip");
        assertTrue(zipFile.exists(), "Zip file should be created");

        manifestFile.delete();
        jsonDir.delete();
        zipFile.delete();
    }

    @Test
    void testGenerateManifest() throws Exception {
        File jsonDir = new File("Json_InitialLoad");
        jsonDir.mkdirs();

        ReflectionTestUtils.invokeMethod(processInitialLoadService, "generateManifest", mockFilteredResults);

        String formattedDate = LocalDate.parse(yesterdayDate).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File manifestFile = new File(jsonDir, "manifest-" + formattedDate + ".txt");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        manifestFile.delete();
        jsonDir.delete();
    }

    @Test
    void testZipJsonFilteredDirectory() throws Exception {
        File jsonDir = new File("Json_InitialLoad");
        jsonDir.mkdirs();
        File testFile = new File(jsonDir, "test.txt");
        testFile.createNewFile();

        ReflectionTestUtils.invokeMethod(processInitialLoadService, "zipJsonFilteredDirectory");

        File zipFile = new File("Json_InitialLoad.zip");
        assertTrue(zipFile.exists(), "Zip file should be created");

        testFile.delete();
        jsonDir.delete();
        zipFile.delete();
    }

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