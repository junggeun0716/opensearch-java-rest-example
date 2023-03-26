import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchAsyncBulkIndexer implements AsyncIndexer<IndexRequest> {

  private static final Logger logger = LoggerFactory.getLogger(OpensearchAsyncIndexer.class);

  private final BulkProcessor bulkProcessor;
  private final int bulkActions;
  private final BackoffPolicy backoffPolicy;
  private final String username;
  private final String password;
  private final List<HttpHost> hosts;
  private final RestHighLevelClient client;

  public OpensearchAsyncBulkIndexer(int bulkActions, BackoffPolicy backoffPolicy,
      List<CountDownLatch> countDownLatches, String username, String password, HttpHost... hosts) {
    this.bulkActions = bulkActions;
    this.backoffPolicy = backoffPolicy;
    this.bulkProcessor = createBulkProcessor(countDownLatches);
    this.username = username;
    this.password = password;
    this.hosts = Arrays.asList(hosts);
    this.client = new RestHighLevelClient(createRestClientBuilder());
  }

  @Override
  public void index(IndexRequest request) {
    bulkProcessor.add(request);
  }

  @Override
  public void close() throws Exception {
    bulkProcessor.flush();
    client.close();
  }

  public void flush() {
    bulkProcessor.flush();
  }

  private RestClientBuilder createRestClientBuilder() {
    RestClientBuilder restClientBuilder = RestClient.builder(hosts.toArray(new HttpHost[0]));

    restClientBuilder.setHttpClientConfigCallback(
        httpClientBuilder -> {
          final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(
              AuthScope.ANY, new UsernamePasswordCredentials(username, password)
          );

          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
    );

    return restClientBuilder;
  }

  private BulkProcessor createBulkProcessor(List<CountDownLatch> countDownLatches) {
    final BulkProcessor.Builder builder =
        BulkProcessor.builder(
            new BiConsumer<BulkRequest, ActionListener<BulkResponse>>() {
              @Override
              public void accept(
                  BulkRequest bulkRequest,
                  ActionListener<BulkResponse> bulkResponseActionListener) {
                client.bulkAsync(
                    bulkRequest,
                    RequestOptions.DEFAULT,
                    bulkResponseActionListener);
              }
            },
            new BulkListener(countDownLatches));

    builder.setBulkActions(bulkActions);
    builder.setBackoffPolicy(backoffPolicy);
    return builder.build();
  }

  private class BulkListener implements BulkProcessor.Listener {

    private List<CountDownLatch> countDownLatches;

    private BulkListener(List<CountDownLatch> countDownLatches) {
      this.countDownLatches = countDownLatches;
    }

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {

    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
        BulkResponse response) {
      logger.info("Asynchronous bulk indexing request succeeded.");
      for (CountDownLatch countDownLatch : countDownLatches) {
        countDownLatch.countDown();
      }
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request,
        Throwable failure) {
      logger.error("Asynchronous bulk indexing request failed.");
      for (CountDownLatch countDownLatch : countDownLatches) {
        countDownLatch.countDown();
      }
    }
  }
}
