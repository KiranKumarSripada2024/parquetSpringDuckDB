package com.example.parquetTest.service;

import org.springframework.stereotype.Service;
import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Service
public class ExtractionService {

    public Map<String, List<byte[]>> extractParquetFromZip(File zipFile) throws IOException {
        Map<String, List<byte[]>> parquetFiles = new HashMap<>();

        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().endsWith(".parquet")) {
                    String folder = entry.getName().split("/")[0];

                    // Read parquet file into memory as byte array
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = zis.read(buffer)) != -1) {
                        baos.write(buffer, 0, bytesRead);
                    }

                    parquetFiles.computeIfAbsent(folder, k -> new ArrayList<>()).add(baos.toByteArray());
                }
            }
        }
        return parquetFiles;
    }
}
