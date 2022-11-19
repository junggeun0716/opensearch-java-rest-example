public interface AsyncIndexer<RequestT> extends AutoCloseable {
  void index(RequestT request);
}
