import java.io.IOException;

public interface AsyncIndexer<Request> extends AutoCloseable {

  void index(Request request) throws IOException;
}
