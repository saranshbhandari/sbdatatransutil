import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

private List<ColumnDetail> parseCsv(InputStream in, boolean isFirstRowHeader) throws Exception {
    // Create a reader and parser
    try (Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
         CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT)) {

        List<CSVRecord> records = parser.getRecords();
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        // The "header" record (either true header or first data row)
        CSVRecord headerRec = records.get(0);

        // Build header names
        List<String> headers = new ArrayList<>();
        if (isFirstRowHeader) {
            for (int i = 0; i < headerRec.size(); i++) {
                headers.add(headerRec.get(i));
            }
        } else {
            for (int i = 0; i < headerRec.size(); i++) {
                headers.add(String.valueOf(i));
            }
        }

        // Pick a sample row for type detection
        CSVRecord sampleRec = (isFirstRowHeader && records.size() > 1)
                ? records.get(1)
                : headerRec;

        // Build the output list
        List<ColumnDetail> out = new ArrayList<>();
        for (int i = 0; i < headers.size(); i++) {
            String sample = i < sampleRec.size() ? sampleRec.get(i) : "";
            String type = DataTypeDetector.detect(sample);
            out.add(new ColumnDetail(headers.get(i), type));
        }

        return out;
    }
}
