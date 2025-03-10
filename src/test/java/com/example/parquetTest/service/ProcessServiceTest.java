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
public class ProcessServiceTest {

    @Mock
    private DownloadService downloadService;

    @Mock
    private ExtractionService extractionService;

    @Mock
    private FilterService filterService;

    @InjectMocks
    private ProcessService processService;

    private File mockZipFile;
    private Map<String, List<byte[]>> mockParquetFiles;
    private Map<String, ProcessService.FilterResult> mockFilteredResults;
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
        mockFilteredResults.put("asset", createMockFilterResult("asset", 10));
        mockFilteredResults.put("view_events", createMockFilterResult("view_events", 20));
    }

    @Test
    void testProcess() throws Exception {
        when(downloadService.downloadZip()).thenReturn(mockZipFile);
        when(extractionService.extractParquetFromZip(mockZipFile)).thenReturn(mockParquetFiles);
        when(filterService.filterParquetFiles(mockParquetFiles)).thenReturn(mockFilteredResults);

        processService.process();

        verify(downloadService, times(1)).downloadZip();
        verify(extractionService, times(1)).extractParquetFromZip(mockZipFile);
        verify(filterService, times(1)).filterParquetFiles(mockParquetFiles);

        File jsonDir = new File("Json_filtered");
        assertTrue(jsonDir.exists(), "JSON directory should be created");

        String formattedDate = LocalDate.parse(yesterdayDate).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File manifestFile = new File(jsonDir, "manifest-" + formattedDate + ".txt");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        File zipFile = new File("Json_filtered.zip");
        assertTrue(zipFile.exists(), "Zip file should be created");

        manifestFile.delete();
        jsonDir.delete();
        zipFile.delete();
    }

    @Test
    void testSaveJsonOutput() throws Exception {
        ReflectionTestUtils.invokeMethod(processService, "saveJsonOutput", mockFilteredResults);

        File jsonDir = new File("Json_filtered");
        assertTrue(jsonDir.exists(), "JSON directory should be created");

        String formattedDate = LocalDate.parse(yesterdayDate).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File assetJsonFile = new File(jsonDir, "asset-" + formattedDate + ".json");
        File viewEventsJsonFile = new File(jsonDir, "view_events-" + formattedDate + ".json");

        assertTrue(assetJsonFile.exists(), "Asset JSON file should be created");
        assertTrue(viewEventsJsonFile.exists(), "View events JSON file should be created");

        assetJsonFile.delete();
        viewEventsJsonFile.delete();
        jsonDir.delete();
    }

    @Test
    void testGenerateManifest() throws Exception {
        ReflectionTestUtils.invokeMethod(processService, "generateManifest", mockFilteredResults);

        File jsonDir = new File("Json_filtered");
        assertTrue(jsonDir.exists(), "JSON directory should be created");

        String formattedDate = LocalDate.parse(yesterdayDate).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File manifestFile = new File(jsonDir, "manifest-" + formattedDate + ".txt");
        assertTrue(manifestFile.exists(), "Manifest file should be created");

        manifestFile.delete();
        jsonDir.delete();
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

    private ProcessService.FilterResult createMockFilterResult(String folderName, int recordCount) {
        ProcessService.FilterResult result = new ProcessService.FilterResult(folderName, yesterdayDate);

        Map<String, Object> row1 = new HashMap<>();
        row1.put("id", "1");
        row1.put("name", "Test Name");
        row1.put("value", 100);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("id", "2");
        row2.put("name", "Another Test");
        row2.put("value", 200);

        result.addData(row1);
        result.addData(row2);
        result.addFile("test.parquet", recordCount);

        return result;
    }
}
