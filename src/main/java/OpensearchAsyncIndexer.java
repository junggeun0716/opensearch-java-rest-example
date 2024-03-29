import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpensearchAsyncIndexer implements AsyncIndexer<IndexRequest> {

  private static final Logger logger = LoggerFactory.getLogger(OpensearchAsyncIndexer.class);

  private final ActionListener actionListener;
  private final String username;
  private final String password;
  private final List<HttpHost> hosts;
  private final RestHighLevelClient client;

  public OpensearchAsyncIndexer(List<CountDownLatch> countDownLatches, String username,
      String password, HttpHost... hosts) {
    this.actionListener = new Listener(countDownLatches);
    this.username = username;
    this.password = password;
    this.hosts = Arrays.asList(hosts);
    this.client = new RestHighLevelClient(createRestClientBuilder());
  }

  @Override
  public void index(IndexRequest request) {
    client.indexAsync(request, RequestOptions.DEFAULT, actionListener);
  }

  @Override
  public void close() throws Exception {
    client.close();
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

  private class Listener implements ActionListener<IndexResponse> {

    private List<CountDownLatch> countDownLatches;

    private Listener(List<CountDownLatch> countDownLatches) {
      this.countDownLatches = countDownLatches;
    }

    @Override
    public void onResponse(IndexResponse indexResponse) {
      logger.info("Asynchronous indexing request succeeded.");
      for (CountDownLatch countDownLatch : countDownLatches) {
        countDownLatch.countDown();
      }
    }

    @Override
    public void onFailure(Exception e) {
      logger.error("Asynchronous indexing request failed.", e);
      for (CountDownLatch countDownLatch : countDownLatches) {
        countDownLatch.countDown();
      }
    }
  }
}
