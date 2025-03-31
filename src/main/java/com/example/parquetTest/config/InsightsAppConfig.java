package com.example.parquetTest.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Configuration
public class InsightsAppConfig {

    @Value("${app.download.dir}")
    private String downloadDir;

    @Value("${app.json.dir1}")
    private String jsonDir1;

    @Value("${app.json.dir2}")
    private String jsonDir2;

    @Value("${app.duckdb.file}")
    private String duckDbFile;

    @Value("${app.date.frequency}")
    private int dateOffset;


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

    public int getDateOffset() {
        return dateOffset;
    }

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public String getJsonZip1(){
        String date = getFormattedDateWithOffsetZip();
        return downloadDir+"/"+jsonDir1+"-"+date+".zip";
    }

    public String getJsonZip2(){
        String date = getFormattedDateWithOffsetZip();
        return downloadDir+"/"+jsonDir2+"-"+date+".zip";
    }

    public String getFormattedDateWithOffset() {
        return LocalDate.now().minusDays(dateOffset).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    public String getFormattedDateWithOffsetZip() {
        return LocalDate.now().minusDays(dateOffset).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    public String getDateWithOffset() {
        return LocalDate.now().minusDays(dateOffset).toString();
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
        return objectMapper;
    }

    @Bean
    public CloseableHttpClient httpClient() {
        return HttpClients.createDefault();
    }
}