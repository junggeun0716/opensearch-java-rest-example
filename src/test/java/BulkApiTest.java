import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContexts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class BulkApiTest {
  @Container
  private OpensearchContainer opensearch = new OpensearchContainer(
      DockerImageName.parse("opensearchproject/opensearch:1.2.4"))
      .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms2g -Xmx2g");

  private RestHighLevelClient client;

  @BeforeEach
  void setUp() {
    client = new RestHighLevelClient(
        RestClient.builder(HttpHost.create(opensearch.getHttpHostAddress()))
            .setHttpClientConfigCallback(
                httpClientBuilder -> {
                  try {
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(
                        AuthScope.ANY, new UsernamePasswordCredentials(opensearch.getUsername(),
                            opensearch.getPassword()));

                    return httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setSSLContext(
                            SSLContexts.custom()
                                .loadTrustMaterial(new TrustAllStrategy())
                                .build());
                  } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
                    throw new RuntimeException(e);
                  }
                }
            )
    );
  }

  @AfterEach
  void tearDown() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testBulkTest() throws IOException, InterruptedException {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(opensearch.getUsername(), opensearch.getPassword()));

    RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(HttpHost.create(opensearch.getHttpHostAddress()))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            })
    );

    BulkRequest request = new BulkRequest();

    request.add(new IndexRequest("posts").id("1").source(Map.of("field1", "foo", "field2", 1)));
    request.add(new IndexRequest("posts").id("2").source(Map.of("field1", "bar", "field2", 2)));
    request.add(new IndexRequest("posts").id("3").source(Map.of("field1", "baz", "field2", 3)));

    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
    System.out.println(bulkResponse.hasFailures());

    GetResponse getResponse = client.get(new GetRequest("posts", "1"), RequestOptions.DEFAULT);
    System.out.println(getResponse.getSource());

    ActionListener listener = new ActionListener<IndexResponse>() {
      @Override
      public void onResponse(IndexResponse indexResponse) {
        System.out.println("tt");
      }

      @Override
      public void onFailure(Exception e) {

      }
    };
    client.indexAsync(new IndexRequest("posts").id("1").source(Map.of("field1", "foo", "field2", 1)), RequestOptions.DEFAULT, listener);
    Thread.sleep(1000);
  }
}
