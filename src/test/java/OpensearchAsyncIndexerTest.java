import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class OpensearchAsyncIndexerTest {

  @Container
  private static final OpensearchContainer OS_CONTAINER = new OpensearchContainer(
      DockerImageName.parse("opensearchproject/opensearch:1.2.4"))
      .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms2g -Xmx2g");

  private RestHighLevelClient client;

  @BeforeEach
  void setUp() {
    client = new RestHighLevelClient(
        RestClient.builder(HttpHost.create(OS_CONTAINER.getHttpHostAddress()))
            .setHttpClientConfigCallback(
                httpClientBuilder -> {
                  final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                  credentialsProvider.setCredentials(
                      AuthScope.ANY, new UsernamePasswordCredentials(OS_CONTAINER.getUsername(),
                          OS_CONTAINER.getPassword()));

                  return httpClientBuilder
                      .setDefaultCredentialsProvider(credentialsProvider);
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
  public void testOpensearchAsyncIndexer() throws IOException, InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    OpensearchAsyncIndexer opensearchAsyncIndexer = new OpensearchAsyncIndexer(List.of(countDownLatch),
        OS_CONTAINER.getUsername(), OS_CONTAINER.getPassword(), HttpHost.create(OS_CONTAINER.getHttpHostAddress()));
    IndexRequest indexRequest = new IndexRequest("testindex").id("1")
        .source(Map.of("field1", "foo", "field2", 1));
    opensearchAsyncIndexer.index(indexRequest);
    countDownLatch.await();

    GetResponse getResponse = client.get(new GetRequest("testindex", "1"), RequestOptions.DEFAULT);
    assertEquals("foo", getResponse.getSource().get("field1"));
    assertEquals(1, getResponse.getSource().get("field2"));
  }
}
