package com.example.demo.service;

import com.example.demo.dto.ColumnDetail;
import com.example.demo.util.DataTypeDetector;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.*;

@Service
public class FileProcessingService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public List<ColumnDetail> extractColumns(MultipartFile file, boolean isFirstRowHeader) throws Exception {
        String name = file.getOriginalFilename().toLowerCase();
        if (name.endsWith(".csv")) {
            return parseCsv(file.getInputStream(), isFirstRowHeader);
        } else if (name.endsWith(".xls") || name.endsWith(".xlsx")) {
            return parseExcel(file.getInputStream(), isFirstRowHeader);
        } else if (name.endsWith(".json")) {
            return parseJson(file.getInputStream(), isFirstRowHeader);
        } else {
            throw new IllegalArgumentException("Unsupported file type");
        }
    }

    private List<ColumnDetail> parseCsv(InputStream in, boolean hdr) throws Exception {
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(
            new java.io.InputStreamReader(in));
        Iterator<CSVRecord> it = records.iterator();
        if (!it.hasNext()) return Collections.emptyList();

        CSVRecord first = it.next();
        List<String> headers = new ArrayList<>();
        if (hdr) {
            first.forEach(headers::add);
        } else {
            for (int i = 0; i < first.size(); i++) headers.add(String.valueOf(i));
        }

        // collect first non-null row for type inference
        CSVRecord dataRow = hdr && it.hasNext() ? it.next() : first;
        List<ColumnDetail> out = new ArrayList<>();
        for (int i = 0; i < headers.size(); i++) {
            String sample = dataRow.size() > i ? dataRow.get(i) : "";
            String type = DataTypeDetector.detect(sample);
            out.add(new ColumnDetail(headers.get(i), type));
        }
        return out;
    }

    private List<ColumnDetail> parseExcel(InputStream in, boolean hdr) throws Exception {
        Workbook wb = WorkbookFactory.create(in);
        Sheet sheet = wb.getSheetAt(0);
        Iterator<Row> rows = sheet.rowIterator();
        if (!rows.hasNext()) return Collections.emptyList();

        Row first = rows.next();
        List<String> headers = new ArrayList<>();
        for (Cell cell : first) {
            if (hdr) headers.add(cell.getStringCellValue());
            else headers.add(String.valueOf(cell.getColumnIndex()));
        }

        Row dataRow = hdr && rows.hasNext() ? rows.next() : first;
        List<ColumnDetail> out = new ArrayList<>();
        for (int i = 0; i < headers.size(); i++) {
            Cell c = dataRow.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
            String sample = DataTypeDetector.cellValueAsString(c);
            String type = DataTypeDetector.detect(sample);
            out.add(new ColumnDetail(headers.get(i), type));
        }
        return out;
    }

    private List<ColumnDetail> parseJson(InputStream in, boolean hdr) throws Exception {
        JsonNode root = objectMapper.readTree(in);
        if (!root.isArray() || root.size() == 0) return Collections.emptyList();

        List<JsonNode> rows = new ArrayList<>();
        root.forEach(rows::add);

        Set<String> keys;
        if (hdr) {
            keys = new LinkedHashSet<>(rows.get(0).fieldNames().toList());
        } else {
            // if no header, use positions 0..n-1 from first object
            int count = rows.get(0).size();
            keys = new LinkedHashSet<>();
            for (int i = 0; i < count; i++) keys.add(String.valueOf(i));
        }

        JsonNode sampleNode = (hdr ? rows.get(0) : rows.get(0)); 
        List<ColumnDetail> out = new ArrayList<>();
        for (String key : keys) {
            JsonNode v = hdr ? sampleNode.get(key) : sampleNode.elements().hasNext() ? 
                             sampleNode.elements().next() : null;
            String sample = (v != null && !v.isNull()) ? v.asText() : "";
            String type = DataTypeDetector.detect(sample);
            out.add(new ColumnDetail(key, type));
        }
        return out;
    }
}
