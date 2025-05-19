@Override
public void processTask(DataTaskSettings settings) throws Exception {
    // 1) Instantiate reader & writer
    DataReader reader = createReader(settings);
    DataWriter writer = createWriter(settings);

    try (reader; writer) {
        reader.open(settings);
        writer.open(settings);

        if (settings.isParallelProcessing()) {
            // --- PARALLEL STREAMING MODE ---
            int threads   = Runtime.getRuntime().availableProcessors();
            int maxQueue  = threads * 2;                       // cap memory footprint
            ExecutorService exec = Executors.newFixedThreadPool(threads);
            CompletionService<Void> completion = new ExecutorCompletionService<>(exec);

            int outstanding = 0;
            boolean errorOccurred = false;
            Exception errorEx = null;

            List<Map<String, Object>> batch;
            while (!errorOccurred
                   && (batch = reader.readBatch()) != null
                   && !batch.isEmpty()) {

                // apply optional transformer/filter
                batch = transformBatch(batch, settings.getTransformer());
                if (batch.isEmpty()) {
                    continue;
                }

                // submit the write task
                completion.submit(() -> {
                    writer.writeBatch(batch);
                    return null;
                });
                outstanding++;

                // throttle: wait for one to finish if too many outstanding
                if (outstanding >= maxQueue) {
                    Future<Void> f = completion.take();  // blocks until one completes
                    try {
                        f.get();
                    } catch (ExecutionException | InterruptedException ex) {
                        errorOccurred = true;
                        errorEx = ex instanceof ExecutionException
                                  ? (Exception) ex.getCause()
                                  : ex;
                    }
                    outstanding--;
                }
            }

            // wait for any remaining tasks
            while (!errorOccurred && outstanding > 0) {
                Future<Void> f = completion.take();
                try {
                    f.get();
                } catch (ExecutionException | InterruptedException ex) {
                    errorOccurred = true;
                    errorEx = ex instanceof ExecutionException
                              ? (Exception) ex.getCause()
                              : ex;
                }
                outstanding--;
            }

            // shut down the executor
            exec.shutdownNow();

            // commit or rollback
            if (errorOccurred) {
                if (settings.isRollbackOnError()) {
                    writer.rollback();
                }
                throw errorEx;
            } else {
                writer.commit();
            }

        } else {
            // --- SEQUENTIAL MODE (unchanged) ---
            List<Map<String, Object>> batch;
            boolean errorOccurred = false;
            Exception errorEx = null;

            while ((batch = reader.readBatch()) != null && !batch.isEmpty()) {
                batch = transformBatch(batch, settings.getTransformer());
                if (batch.isEmpty()) continue;
                try {
                    writer.writeBatch(batch);
                } catch (Exception ex) {
                    errorOccurred = true;
                    errorEx = ex;
                    break;
                }
            }

            if (errorOccurred) {
                if (settings.isRollbackOnError()) {
                    writer.rollback();
                }
                throw errorEx;
            } else {
                writer.commit();
            }
        }
    }
}

/** Factory methods (for clarity) **/
private DataReader createReader(DataTaskSettings s) { /* switch on s.getSourceType() */ }
private DataWriter createWriter(DataTaskSettings s) { /* switch on s.getDestType()   */ }

/** transformBatch(...) remains the same **/
private List<Map<String, Object>> transformBatch(
    List<Map<String, Object>> batch,
    RecordTransformer transformer
) {
    if (transformer == null) return batch;
    List<Map<String, Object>> out = new ArrayList<>();
    for (var rec : batch) {
        var tr = transformer.transform(rec);
        if (tr != null) out.add(tr);
    }
    return out;
}
