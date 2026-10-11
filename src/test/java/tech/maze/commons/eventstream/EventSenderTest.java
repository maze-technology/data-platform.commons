package tech.maze.commons.eventstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.StringValue;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.retry.support.RetryTemplate;

class EventSenderTest {
  @Test
  void resolvesReplyToWhenPresent() {
    EventSender sender = new EventSender(
        Mockito.mock(StreamBridge.class),
        eventProperties(),
        RetryTemplate.builder().maxAttempts(1).build()
    );

    CloudEvent event = CloudEventBuilder.v1()
        .withId("id")
        .withSource(URI.create("urn:test"))
        .withType("type")
        .withExtension(EventSender.REPLY_TO_EXTENSION, "queue")
        .build();

    assertEquals("queue", sender.resolveReplyTo(event));
    assertNull(sender.resolveReplyTo(null));
  }

  @Test
  void sendPublishesEvent() {
    StreamBridge streamBridge = Mockito.mock(StreamBridge.class);
    when(streamBridge.send(eq("topic"), any(CloudEvent.class))).thenReturn(true);

    EventSender sender = new EventSender(
        streamBridge,
        eventProperties(),
        RetryTemplate.builder().maxAttempts(1).build()
    );

    boolean sent = sender.send("topic", StringValue.of("hello"));

    assertTrue(sent);
    verify(streamBridge).send(eq("topic"), any(CloudEvent.class));
  }

  @Test
  void sendReturnsFalseWhenBridgeFails() {
    StreamBridge streamBridge = Mockito.mock(StreamBridge.class);
    when(streamBridge.send(eq("topic"), any(CloudEvent.class))).thenReturn(false);

    EventSender sender = new EventSender(
        streamBridge,
        eventProperties(),
        RetryTemplate.builder().maxAttempts(1).build()
    );

    assertFalse(sender.send("topic", StringValue.of("hello")));
  }

  @Test
  void sendRejectsInvalidArguments() {
    EventSender sender = new EventSender(
        Mockito.mock(StreamBridge.class),
        eventProperties(),
        RetryTemplate.builder().maxAttempts(1).build()
    );

    assertThrows(IllegalArgumentException.class, () -> sender.send(" ", StringValue.of("hi")));
    assertThrows(IllegalArgumentException.class, () -> sender.send("topic", null));
  }

  @Test
  void sendWithExplicitTypeAndExtensions() {
    StreamBridge streamBridge = Mockito.mock(StreamBridge.class);
    when(streamBridge.send(eq("markets.fetch.request"), any(CloudEvent.class))).thenReturn(true);

    EventSender sender = new EventSender(
        streamBridge,
        eventProperties(),
        RetryTemplate.builder().maxAttempts(1).build()
    );

    boolean sent = sender.send(
        "markets.fetch.request",
        StringValue.of("fetch"),
        "tech.maze.markets.fetch.request",
        Map.of(
            EventSender.CORRELATION_ID_EXTENSION, "corr-1",
            EventSender.REPLY_TO_EXTENSION, "markets.fetch.response"
        )
    );

    assertTrue(sent);
    verify(streamBridge).send(eq("markets.fetch.request"), any(CloudEvent.class));
  }

  @Test
  void resolvesCorrelationIdWhenPresent() {
    EventSender sender = new EventSender(
        Mockito.mock(StreamBridge.class),
        eventProperties(),
        RetryTemplate.builder().maxAttempts(1).build()
    );

    CloudEvent event = CloudEventBuilder.v1()
        .withId("id")
        .withSource(URI.create("urn:test"))
        .withType("type")
        .withExtension(EventSender.CORRELATION_ID_EXTENSION, "corr-42")
        .build();

    assertEquals("corr-42", sender.resolveCorrelationId(event));
    assertNull(sender.resolveCorrelationId(null));
  }

  private MazeEventProperties eventProperties() {
    MazeEventProperties properties = new MazeEventProperties();
    properties.setSource(URI.create("urn:service:hello-world"));
    return properties;
  }
}
