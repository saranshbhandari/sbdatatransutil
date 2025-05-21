private List<ColumnDetail> parseJson(InputStream in, boolean hdr) throws Exception {
    JsonNode root = objectMapper.readTree(in);
    if (!root.isArray() || root.size() == 0) {
        return Collections.emptyList();
    }

    // Collect all elements into a List<JsonNode>
    List<JsonNode> rows = new ArrayList<>();
    root.forEach(rows::add);

    // Determine headers
    List<String> headers = new ArrayList<>();
    JsonNode first = rows.get(0);
    if (hdr) {
        // Collect field names from the first object
        Iterator<String> fieldNames = first.fieldNames();
        fieldNames.forEachRemaining(headers::add);
    } else {
        // Use numeric indices as headers
        int count = first.size();
        for (int i = 0; i < count; i++) {
            headers.add(String.valueOf(i));
        }
    }

    // Prepare output list
    List<ColumnDetail> out = new ArrayList<>();
    for (int i = 0; i < headers.size(); i++) {
        String header = headers.get(i);
        String sample;
        if (hdr) {
            JsonNode v = first.get(header);
            sample = (v != null && !v.isNull()) ? v.asText() : "";
        } else {
            JsonNode v = first.path(i);
            sample = v.isMissingNode() || v.isNull() ? "" : v.asText();
        }
        String type = DataTypeDetector.detect(sample);
        out.add(new ColumnDetail(header, type));
    }

    return out;
}
