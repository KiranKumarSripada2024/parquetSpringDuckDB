package com.example.parquetTest.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Value("${app.download.url}")
    private String downloadUrl;

    @Value("${app.download.dir}")
    private String downloadDir;

    @Value("${app.json.dir}")
    private String jsonDir;

    @Value("${app.duckdb.file}")
    private String duckDbFile;

    @Value("${app.username}")
    private String username;

    @Value("${app.password}")
    private String password;

    public String getDownloadUrl() {
        return downloadUrl;
    }

    public String getDownloadDir() {
        return downloadDir;
    }

    public String getJsonDir() {
        return jsonDir;
    }

    public String getDuckDbFile() {
        return duckDbFile;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}

