package com.gerlowskija.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Runs a ~very basic~ SolrJ performance experiment that tests the throughput (in docs/sec) of each SolrClient with a
 * variety of batch sizes.
 *
 * The experiment data is printed to stdout in a CSV format ideal for examination and manipulation by your favorite
 * spreadsheet software.  The first entry in each line shows the batch size used in this particular 'sub-experiment'.
 * The following three entries on each line show the average throughput in docs/second of the HttpSolrClient,
 * ConcurrentUpdateSolrClient and CloudSolrClient (respectively).
 *
 * One limitation of the current skeleton is that it doesn't separate out the time required to create the documents you
 * want to index from the time it takes to actually index them.  This doesn't skew results much since the current
 * documents are trivial (just a UUID).  But anyone modifying this template to use less-trivial file generation (e.g.
 * reading files from disk) should change the time-measurement from using the System clock to using a Stopwatch class
 * (such as is available in Apache Commons or Guava) that may be stopped and then resumed while each batch is being
 * constructed.
 */
public class SolrJBatchTester {
    private static final int MAX_BATCH_SIZE = 1000;
    private static final int TOTAL_NUM_DOCS = 500000; //500K
    private static final String SOLR_BASE_URL = "http://localhost:8983/solr";
    private static final String ZK_HOST = "localhost:9983";
    private static final String SOLR_PACKAGE_DIRECTORY = "/path/to/installed/solr";
    private static final String COLLECTION_NAME = "perf_test_collection";
    private static final int NUM_SHARDS = 2;
    private static final int NUM_REPLICAS = 2;

    public static void main(String[] args) throws Exception {
        final List<StringBuilder> perfResults = new ArrayList<>();
        perfResults.add(new StringBuilder("BatchSize, HttpSolrClient, ConcurrentUpdateSolrClient, CloudSolrClient"));
        startSolr();
        deleteCollection();
        createCollection();

        System.out.println("BatchSize, HttpSolrClient, ConcurrentUpdateSolrClient, CloudSolrClient");
        for (int currentBatchSize = 1; currentBatchSize < MAX_BATCH_SIZE; currentBatchSize++) {
            perfResults.add(new StringBuilder(currentBatchSize + ", "));
            deleteCollection();
            createCollection();
            try(SolrClient client1 = createHttpClient()) {
                runIndexingTest(perfResults, currentBatchSize, client1);
            }

            perfResults.get(currentBatchSize).append(", ");
            deleteCollection();
            createCollection();
            try(SolrClient client1 = createConcurrentUpdateSolrClient()) {
                runIndexingTest(perfResults, currentBatchSize, client1);
            }


            perfResults.get(currentBatchSize).append(", ");
            deleteCollection();
            createCollection();
            try(SolrClient client1 = createCloudSolrClient()) {
                runIndexingTest(perfResults, currentBatchSize, client1);
            }
            System.out.println(perfResults.get(currentBatchSize));
        }
        stopSolr();
    }

    private static void runCommand(String... command) throws Exception {
        ProcessBuilder b = new ProcessBuilder(command)
                .directory(new File(SOLR_PACKAGE_DIRECTORY))
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Process p = b.start();
        p.waitFor();
    }

    private static void startSolr() throws Exception {
        runCommand("bin/solr", "start", "-e", "cloud", "-noprompt");
    }



    private static void deleteCollection() throws Exception {
        runCommand("bin/solr", "delete", "-c", COLLECTION_NAME);
    }

    private static void createCollection() throws Exception  {
        runCommand("bin/solr", "create_collection", "-c", COLLECTION_NAME, "-shards", String.valueOf(NUM_SHARDS), "-replicationFactor", String.valueOf(NUM_REPLICAS));
    }

    private static void stopSolr()  throws Exception {
        runCommand("bin/solr", "stop", "-all");
    }

    private static void runIndexingTest(List<StringBuilder> resultsContainer, int batchSize, SolrClient client) throws Exception {
            int docCount = 0;
            long startMillis = System.currentTimeMillis();
            while (docCount < TOTAL_NUM_DOCS) {
                List<SolrInputDocument> currentBatch = createBatch(batchSize, TOTAL_NUM_DOCS, docCount);
                client.add(COLLECTION_NAME, currentBatch);
                docCount += currentBatch.size();
            }
            client.commit(COLLECTION_NAME);
            long finishMillis = System.currentTimeMillis();
            double durationSeconds = ((double)(finishMillis - startMillis) / (double) 1000);
            double docsPerSec = (double)TOTAL_NUM_DOCS / (double)durationSeconds;
            resultsContainer.get(batchSize).append(docsPerSec);
    }

    private static SolrClient createHttpClient() {
            return new HttpSolrClient.Builder(SOLR_BASE_URL)
                    .build();
    }

    private static SolrClient createConcurrentUpdateSolrClient() {
        return new ConcurrentUpdateSolrClient.Builder(SOLR_BASE_URL)
                .build();
    }

    private static SolrClient createCloudSolrClient() {
        final List<String> zkUrls = new ArrayList<>();
        zkUrls.add(ZK_HOST);
        return new CloudSolrClient.Builder(zkUrls, Optional.empty())
                .build();
    }

    private static List<SolrInputDocument> createBatch(int batchSize, int totalNumDocs, int currentDocCount) {
        final List<SolrInputDocument> batch = new ArrayList<>();
        int numToCreate = (currentDocCount + batchSize > totalNumDocs) ? totalNumDocs - currentDocCount : batchSize;
        for (int i = 1; i <= numToCreate; i++) {
            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", String.valueOf(currentDocCount + i));
            doc.addField("text", UUID.randomUUID().toString());
            batch.add(doc);
        }

        return batch;
    }
}
