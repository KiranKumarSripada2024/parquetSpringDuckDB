package com.example.parquetTest.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Value("${app.download.url}")
    private String downloadUrl;

    @Value("${app.download.dir}")
    private String downloadDir;

    @Value("${app.json.dir1}")
    private String jsonDir1;

    @Value("${app.json.dir2}")
    private String jsonDir2;

    @Value("${app.duckdb.file}")
    private String duckDbFile;

    @Value("${app.username}")
    private String username;

    public String getPassword() {
        return password;
    }

    public String getDownloadUrl() {
        return downloadUrl;
    }

    public String getDownloadDir() {
        return downloadDir;
    }

    public String getJsonDir1() {
        return jsonDir1;
    }

    public String getJsonDir2() {
        return jsonDir2;
    }

    public String getDuckDbFile() {
        return duckDbFile;
    }

    public String getUsername() {
        return username;
    }

    @Value("${app.password}")
    private String password;




}

