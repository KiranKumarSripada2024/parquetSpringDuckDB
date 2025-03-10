package com.example.parquetTest.service;

import com.example.parquetTest.utils.DuckDBUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class FilterServiceTest {

    @InjectMocks
    private FilterService filterService;

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private ResultSetMetaData mockMetaData;

    private Map<String, List<byte[]>> testParquetFiles;

    @BeforeEach
    void setUp() throws IOException {
        // Setup test parquet files
        testParquetFiles = new HashMap<>();
        testParquetFiles.put("asset", Arrays.asList(createMockParquetBytes("asset")));
        testParquetFiles.put("view_events", Arrays.asList(createMockParquetBytes("view_events")));
    }

    @Test
    void testFilterParquetFiles() throws Exception {
        try (MockedStatic<DuckDBUtil> mockDuckDBUtil = mockStatic(DuckDBUtil.class)) {
            // Arrange
            mockDuckDBUtil.when(DuckDBUtil::getConnection).thenReturn(mockConnection);
            when(mockConnection.createStatement()).thenReturn(mockStatement);
            when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

            // Mock ResultSet behavior
            when(mockResultSet.next()).thenReturn(true, false, true, false); // First for data, second for count
            when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
            when(mockMetaData.getColumnCount()).thenReturn(3);
            when(mockMetaData.getColumnName(1)).thenReturn("id");
            when(mockMetaData.getColumnName(2)).thenReturn("name");
            when(mockMetaData.getColumnName(3)).thenReturn("value");
            when(mockResultSet.getObject(1)).thenReturn("1");
            when(mockResultSet.getObject(2)).thenReturn("Test Name");
            when(mockResultSet.getObject(3)).thenReturn(100);
            when(mockResultSet.getInt("cnt")).thenReturn(10);

            // Act
            Map<String, ProcessService.FilterResult> result = filterService.filterParquetFiles(testParquetFiles);

            // Assert
            assertNotNull(result);
            assertEquals(2, result.size());
            assertTrue(result.containsKey("asset"));
            assertTrue(result.containsKey("view_events"));

            // Verify asset filter result
            ProcessService.FilterResult assetResult = result.get("asset");
            assertEquals("asset", assetResult.folderName);
            assertEquals(LocalDate.now().minusDays(1).toString(), assetResult.editedDate);
            assertEquals(20, assetResult.totalFilteredRows); // 10 records per file, 2 files
            assertEquals(1, assetResult.data.size());

            // Verify data fields
            Map<String, Object> assetData = assetResult.data.get(0);
            assertEquals("1", assetData.get("id"));
            assertEquals("Test Name", assetData.get("name"));
            assertEquals(100, assetData.get("value"));

            // Verify SQL execution counts
            verify(mockStatement, times(8)).execute(anyString()); // CREATE, ALTER, UPDATE, DROP x 2 files
            verify(mockStatement, times(4)).executeQuery(anyString()); // SELECT data, SELECT count x 2 files
        }
    }

    @Test
    void testFilterParquetFilesWithException() throws Exception {
        try (MockedStatic<DuckDBUtil> mockDuckDBUtil = mockStatic(DuckDBUtil.class)) {
            // Arrange - Simulate an exception
            mockDuckDBUtil.when(DuckDBUtil::getConnection).thenThrow(new SQLException("Test exception"));

            // Act
            Map<String, ProcessService.FilterResult> result = filterService.filterParquetFiles(testParquetFiles);

            // Assert
            assertNotNull(result);
            assertEquals(0, result.size());
        }
    }

    // Helper method to create mock parquet bytes
    private byte[] createMockParquetBytes(String folderName) throws IOException {
        // Create a temporary file to simulate a parquet file
        File tempFile = File.createTempFile("mock_" + folderName, ".parquet");
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(("Mock " + folderName + " Parquet Data").getBytes());
        }
        tempFile.deleteOnExit();
        return tempFile.getPath().getBytes(); // Just return the path as bytes for testing
    }
}