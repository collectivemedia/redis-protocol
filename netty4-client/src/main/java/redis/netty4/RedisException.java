package redis.netty4;

public class RedisException extends RuntimeException {
  public RedisException(String message) {
    super(message);
  }
}
