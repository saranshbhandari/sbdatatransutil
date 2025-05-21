import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.InputStream;
import java.util.*;

private List<ColumnDetail> parseExcel(InputStream in,
                                      String filename,
                                      boolean isFirstRowHeader) throws Exception {
  Workbook wb;
  String lower = filename.toLowerCase(Locale.ROOT);
  if (lower.endsWith(".xls")) {
    wb = new HSSFWorkbook(in);
  } else if (lower.endsWith(".xlsx")) {
    wb = new XSSFWorkbook(in);
  } else {
    throw new IllegalArgumentException("Not an Excel file: " + filename);
  }

  Sheet sheet = wb.getSheetAt(0);
  Iterator<Row> rows = sheet.rowIterator();
  if (!rows.hasNext()) return Collections.emptyList();

  Row first = rows.next();
  // build header names
  List<String> headers = new ArrayList<>();
  for (Cell c : first) {
    headers.add(isFirstRowHeader
        ? c.getStringCellValue()
        : String.valueOf(c.getColumnIndex()));
  }

  // choose sample row
  Row sampleRow = (isFirstRowHeader && rows.hasNext()) ? rows.next() : first;
  List<ColumnDetail> out = new ArrayList<>();
  for (int i = 0; i < headers.size(); i++) {
    Cell c = sampleRow.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
    String text = DataTypeDetector.cellValueAsString(c);
    String type = DataTypeDetector.detect(text);
    out.add(new ColumnDetail(headers.get(i), type));
  }
  return out;
}
