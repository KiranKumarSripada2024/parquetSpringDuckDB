package com.example.parquetTest.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class ExtractionServiceTest {

    @InjectMocks
    private ExtractionService extractionService;

    private File testZipFile;

    @BeforeEach
    void setUp() throws IOException {
        // Create a test ZIP file with mock parquet files
        testZipFile = File.createTempFile("test_zip", ".zip");
        try (FileOutputStream fos = new FileOutputStream(testZipFile);
             ZipOutputStream zos = new ZipOutputStream(fos)) {

            // Add asset parquet file
            ZipEntry assetEntry = new ZipEntry("asset/test_asset.parquet");
            zos.putNextEntry(assetEntry);
            zos.write("Mock Asset Parquet Data".getBytes());
            zos.closeEntry();

            // Add view_events parquet file
            ZipEntry viewEventsEntry = new ZipEntry("view_events/test_view_events.parquet");
            zos.putNextEntry(viewEventsEntry);
            zos.write("Mock View Events Parquet Data".getBytes());
            zos.closeEntry();

            // Add a non-parquet file (should be ignored)
            ZipEntry nonParquetEntry = new ZipEntry("other/test.txt");
            zos.putNextEntry(nonParquetEntry);
            zos.write("This is not a parquet file".getBytes());
            zos.closeEntry();
        }
    }

    @Test
    void testExtractParquetFromZip() throws IOException {
        // Act
        Map<String, List<byte[]>> result = extractionService.extractParquetFromZip(testZipFile);

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("asset"));
        assertTrue(result.containsKey("view_events"));

        // Verify asset parquet data
        List<byte[]> assetParquets = result.get("asset");
        assertEquals(1, assetParquets.size());
        assertEquals("Mock Asset Parquet Data", new String(assetParquets.get(0)));

        // Verify view_events parquet data
        List<byte[]> viewEventsParquets = result.get("view_events");
        assertEquals(1, viewEventsParquets.size());
        assertEquals("Mock View Events Parquet Data", new String(viewEventsParquets.get(0)));

        // Verify that non-parquet file was ignored
        assertFalse(result.containsKey("other"));

        // Clean up
        testZipFile.delete();
    }

    @Test
    void testExtractParquetFromEmptyZip() throws IOException {
        // Create an empty zip file
        File emptyZipFile = File.createTempFile("empty_zip", ".zip");
        try (FileOutputStream fos = new FileOutputStream(emptyZipFile);
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            // Empty zip
        }

        // Act
        Map<String, List<byte[]>> result = extractionService.extractParquetFromZip(emptyZipFile);

        // Assert
        assertNotNull(result);
        assertEquals(0, result.size());

        // Clean up
        emptyZipFile.delete();
    }
}