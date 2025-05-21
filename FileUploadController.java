package com.example.demo.controller;

import com.example.demo.dto.ColumnDetail;
import com.example.demo.service.FileProcessingService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/api/files")
public class FileUploadController {

    private final FileProcessingService service;

    public FileUploadController(FileProcessingService service) {
        this.service = service;
    }

    @PostMapping(value = "/columns",
                 consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<ColumnDetail>> extractColumnDetails(
        @RequestPart("file") MultipartFile file,
        @RequestPart("isFirstRowHeader") boolean isFirstRowHeader
    ) throws Exception {
        List<ColumnDetail> cols = service.extractColumns(file, isFirstRowHeader);
        return ResponseEntity.ok(cols);
    }
}
