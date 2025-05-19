import com.example.datatransfer.model.SourceDestinationMapping;
import com.example.datatransfer.util.DataConverter;

// inside DatabaseWriter class

/**
 * Map sourceRecord → destRecord using your SourceDestinationMapping list,
 * applying explicit type conversion per mapping.destinationColumnType.
 */
private Map<String, Object> applyMappingAndConversion(Map<String, Object> sourceRecord) {
    Map<String, Object> destRecord = new LinkedHashMap<>();
    for (SourceDestinationMapping mapping : settings.getMappings()) {
        String srcField  = mapping.getSourceColumnName();
        String dstField  = mapping.getDestinationColumnName();
        String dstType   = mapping.getDestinationColumnType();

        // Grab raw value from source
        Object rawValue = sourceRecord.get(srcField);

        // Determine target Class from the mapping’s type name
        Class<?> targetClass = mapType(dstType);

        // Convert value (null-safe)
        Object converted = DataConverter.convertValue(rawValue, targetClass);

        destRecord.put(dstField, converted);
    }
    return destRecord;
}

/**
 * Helper to resolve your mapping’s type-name String → Java Class.
 */
private Class<?> mapType(String typeName) {
    if (typeName == null) {
        return Object.class;
    }
    switch (typeName.trim().toLowerCase()) {
        case "string":  return String.class;
        case "int":
        case "integer": return Integer.class;
        case "long":    return Long.class;
        case "double":  return Double.class;
        case "boolean": return Boolean.class;
        case "date":    return java.util.Date.class;
        // add more as needed
        default:        return Object.class;
    }
}
