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
        testZipFile = File.createTempFile("test_zip", ".zip");
        try (FileOutputStream fos = new FileOutputStream(testZipFile);
             ZipOutputStream zos = new ZipOutputStream(fos)) {

            ZipEntry assetEntry = new ZipEntry("asset/test_asset.parquet");
            zos.putNextEntry(assetEntry);
            zos.write("Mock Asset Parquet Data".getBytes());
            zos.closeEntry();

            ZipEntry viewEventsEntry = new ZipEntry("view_events/test_view_events.parquet");
            zos.putNextEntry(viewEventsEntry);
            zos.write("Mock View Events Parquet Data".getBytes());
            zos.closeEntry();

            ZipEntry nonParquetEntry = new ZipEntry("other/test.txt");
            zos.putNextEntry(nonParquetEntry);
            zos.write("This is not a parquet file".getBytes());
            zos.closeEntry();
        }
    }

    @Test
    void testExtractParquetFromZip() throws IOException {
        Map<String, List<byte[]>> result = extractionService.extractParquetFromZip(testZipFile);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("asset"));
        assertTrue(result.containsKey("view_events"));

        List<byte[]> assetParquets = result.get("asset");
        assertEquals(1, assetParquets.size());
        assertEquals("Mock Asset Parquet Data", new String(assetParquets.get(0)));

        List<byte[]> viewEventsParquets = result.get("view_events");
        assertEquals(1, viewEventsParquets.size());
        assertEquals("Mock View Events Parquet Data", new String(viewEventsParquets.get(0)));

        assertFalse(result.containsKey("other"));

        testZipFile.delete();
    }

    @Test
    void testExtractParquetFromEmptyZip() throws IOException {
        File emptyZipFile = File.createTempFile("empty_zip", ".zip");
        try (FileOutputStream fos = new FileOutputStream(emptyZipFile);
             ZipOutputStream zos = new ZipOutputStream(fos)) {
        }

        Map<String, List<byte[]>> result = extractionService.extractParquetFromZip(emptyZipFile);

        assertNotNull(result);
        assertEquals(0, result.size());

        emptyZipFile.delete();
    }
}