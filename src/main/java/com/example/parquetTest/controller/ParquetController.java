package com.example.parquetTest.controller;

import com.example.parquetTest.service.ProcessInitialLoadService;
import com.example.parquetTest.service.ProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/parquet")
public class ParquetController {

    @Autowired
    private ProcessService processService;

    @Autowired
    private ProcessInitialLoadService processInitialLoadService;

    @GetMapping("/process")
    public String processParquetFiles() {
        try {
            processService.process();
            return "Processing completed!";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    @GetMapping("/initialLoad")
    public String processInitialLoadFiles() {
        try {
            processInitialLoadService.process();
            return "Processing completed!";
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

}

