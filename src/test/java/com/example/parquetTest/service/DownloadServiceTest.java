package com.example.parquetTest.service;

import com.example.parquetTest.config.AppConfig;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DownloadServiceTest {

    @Mock
    private AppConfig appConfig;

    @Mock
    private CloseableHttpClient httpClient;

    @Mock
    private CloseableHttpResponse httpResponse;

    @InjectMocks
    private DownloadService downloadService;

    private String testUsername = "testUser";
    private String testPassword = "testPass";
    private String testDownloadUrl = "http://test.url/download?snapshotDate=";
    private String testDownloadDir = "test_downloads";

    @BeforeEach
    void setUp() {
        // Configure app config
        when(appConfig.getUsername()).thenReturn(testUsername);
        when(appConfig.getPassword()).thenReturn(testPassword);
        when(appConfig.getDownloadUrl()).thenReturn(testDownloadUrl);
        when(appConfig.getDownloadDir()).thenReturn(testDownloadDir);

        // Create download directory
        File downloadDir = new File(testDownloadDir);
        if (!downloadDir.exists()) {
            downloadDir.mkdirs();
        }
    }

    @Test
    void testDownloadZipSuccess() throws IOException {
        // Arrange
        String yesterdayDate = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String expectedZipPath = testDownloadDir + File.separator + "insights_" + yesterdayDate + ".zip";
        byte[] zipContent = "test zip content".getBytes();

        try (MockedStatic<org.apache.hc.client5.http.impl.classic.HttpClients> httpClientsMocked = Mockito.mockStatic(org.apache.hc.client5.http.impl.classic.HttpClients.class)) {
            httpClientsMocked.when(org.apache.hc.client5.http.impl.classic.HttpClients::createDefault).thenReturn(httpClient);

            when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);
            when(httpResponse.getCode()).thenReturn(200);

            StringEntity entity = mock(StringEntity.class);
            when(httpResponse.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream(zipContent));

            // Act
            File result = downloadService.downloadZip();

            // Assert
            assertNotNull(result);
            assertEquals(expectedZipPath, result.getPath());
            assertTrue(result.exists());

            // Clean up
            result.delete();
            new File(testDownloadDir).delete();
        }
    }

    @Test
    void testDownloadZipFailure() throws IOException {
        // Arrange
        try (MockedStatic<org.apache.hc.client5.http.impl.classic.HttpClients> httpClientsMocked = Mockito.mockStatic(org.apache.hc.client5.http.impl.classic.HttpClients.class)) {
            httpClientsMocked.when(org.apache.hc.client5.http.impl.classic.HttpClients::createDefault).thenReturn(httpClient);

            when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);
            when(httpResponse.getCode()).thenReturn(404);

            StringEntity entity = mock(StringEntity.class);
            when(httpResponse.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream("Not found".getBytes()));

            // Act & Assert
            assertThrows(IOException.class, () -> downloadService.downloadZip());

            // Clean up
            new File(testDownloadDir).delete();
        }
    }
}