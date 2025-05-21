package com.example.demo.util;

public class DataTypeDetector {

    public static String detect(String sample) {
        if (sample == null || sample.isBlank()) {
            return "String";
        }
        // boolean
        String s = sample.trim().toLowerCase();
        if (s.equals("true") || s.equals("false")) {
            return "Boolean";
        }
        // integer
        try {
            Integer.parseInt(sample);
            return "Integer";
        } catch (Exception ignored) {}
        // decimal
        try {
            Double.parseDouble(sample);
            return "Double";
        } catch (Exception ignored) {}
        // date (very basic ISO check)
        if (sample.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
            return "Date";
        }
        return "String";
    }

    public static String cellValueAsString(org.apache.poi.ss.usermodel.Cell cell) {
        switch (cell.getCellType()) {
            case BOOLEAN: return String.valueOf(cell.getBooleanCellValue());
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toInstant().toString();
                }
                return String.valueOf(cell.getNumericCellValue());
            case STRING: return cell.getStringCellValue();
            case FORMULA: return cell.getCellFormula();
            case BLANK: 
            case _NONE:
            case ERROR:
            default: return "";
        }
    }
}
