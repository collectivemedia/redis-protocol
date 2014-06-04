package redis;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.netty4.RedisReplyDecoder;
import redis.netty4.StatusReply;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class RedisReplyDecoderTest {
  private EmbeddedChannel channel;

  @Before
  public void setup() throws Exception {
    channel = new EmbeddedChannel(new RedisReplyDecoder());
  }

  @After
  public void teardown() throws Exception {
    channel.finish();
  }

  @Test
  public void testDecodeSimpleString() {
    channel.writeInbound(Unpooled.wrappedBuffer("+OK\r\n".getBytes(StandardCharsets.US_ASCII)));
    StatusReply statusReply = (StatusReply) channel.readInbound();
    assertEquals("OK", statusReply.data());
  }

  @Test
  public void testDecodeSimpleString2Chunks() {
    channel.writeInbound(Unpooled.wrappedBuffer("+O".getBytes(StandardCharsets.US_ASCII)));
    channel.writeInbound(Unpooled.wrappedBuffer("K\r\n".getBytes(StandardCharsets.US_ASCII)));
    StatusReply statusReply = (StatusReply) channel.readInbound();
    assertEquals("OK", statusReply.data());
  }

  @Test
  public void testDecode2SimpleStrings() {
    channel.writeInbound(Unpooled.wrappedBuffer("+YES\r\n+NO\r\n".getBytes(StandardCharsets.US_ASCII)));
    assertEquals("YES", ((StatusReply) channel.readInbound()).data());
    assertEquals("NO", ((StatusReply) channel.readInbound()).data());
  }

  @Test
  public void testDecode2SimpleStrings3Chunks() {
    channel.writeInbound(Unpooled.wrappedBuffer("+YE".getBytes(StandardCharsets.US_ASCII)));
    channel.writeInbound(Unpooled.wrappedBuffer("S\r\n+N".getBytes(StandardCharsets.US_ASCII)));
    channel.writeInbound(Unpooled.wrappedBuffer("O\r\n".getBytes(StandardCharsets.US_ASCII)));
    assertEquals("YES", ((StatusReply) channel.readInbound()).data());
    assertEquals("NO", ((StatusReply) channel.readInbound()).data());
  }

  @Test
  public void testDecode2SimpleStrings3Chunks2() {
    channel.writeInbound(Unpooled.wrappedBuffer("+YES\r".getBytes(StandardCharsets.US_ASCII)));
    channel.writeInbound(Unpooled.wrappedBuffer("\n+N".getBytes(StandardCharsets.US_ASCII)));
    channel.writeInbound(Unpooled.wrappedBuffer("O\r\n".getBytes(StandardCharsets.US_ASCII)));
    assertEquals("YES", ((StatusReply) channel.readInbound()).data());
    assertEquals("NO", ((StatusReply) channel.readInbound()).data());
  }

  @Test
  public void testDecode2SimpleStrings3Chunks3() {
    channel.writeInbound(Unpooled.wrappedBuffer("+YES\r\n+".getBytes(StandardCharsets.US_ASCII)));
    channel.writeInbound(Unpooled.wrappedBuffer("NO\r".getBytes(StandardCharsets.US_ASCII)));
    channel.writeInbound(Unpooled.wrappedBuffer("\n".getBytes(StandardCharsets.US_ASCII)));
    assertEquals("YES", ((StatusReply) channel.readInbound()).data());
    assertEquals("NO", ((StatusReply) channel.readInbound()).data());
  }
}
