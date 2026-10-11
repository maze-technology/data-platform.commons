package tech.maze.commons.eventstream;

import static io.cloudevents.protobuf.ProtobufFormat.PROTO_DATA_CONTENT_TYPE;

import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.protobuf.ProtoCloudEventData;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

/**
 * Helper for sending CloudEvents on the event stream.
 */
@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ConditionalOnProperty(prefix = MazeEventProperties.PREFIX, name = "enabled", havingValue = "true")
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "Spring-managed dependencies are safe to store in fields."
)
public class EventSender {
  public static final String REPLY_TO_EXTENSION = "replyto";
  public static final String CORRELATION_ID_EXTENSION = "correlationid";

  StreamBridge streamBridge;
  MazeEventProperties eventProperties;
  RetryTemplate retryTemplate;

  /**
   * Creates an event sender for CloudEvents publishing.
   *
   * @param streamBridge spring cloud stream bridge
   * @param eventProperties event configuration
   * @param retryTemplate retry policy for publishing
   */
  public EventSender(
      StreamBridge streamBridge,
      MazeEventProperties eventProperties,
      RetryTemplate retryTemplate
  ) {
    this.streamBridge = streamBridge;
    this.eventProperties = eventProperties;
    this.retryTemplate = retryTemplate;
  }

  /**
   * Resolves the reply-to destination from an incoming CloudEvent.
   *
   * @param requestEvent the incoming request event
   * @return the reply-to destination or null when not present
   */
  public String resolveReplyTo(CloudEvent requestEvent) {
    return getExtensionAsString(requestEvent, REPLY_TO_EXTENSION);
  }

  /**
   * Resolves the correlation id from an incoming CloudEvent.
   *
   * @param requestEvent the incoming request event
   * @return the correlation id or null when not present
   */
  public String resolveCorrelationId(CloudEvent requestEvent) {
    return getExtensionAsString(requestEvent, CORRELATION_ID_EXTENSION);
  }

  /**
   * Sends a CloudEvent to a destination with a derived type from the message.
   *
   * @param destination the target destination (e.g., Kafka topic)
   * @param message the protobuf payload
   * @return true if the event was sent, false otherwise
   */
  public boolean send(String destination, Message message) {
    if (message == null) {
      throw new IllegalArgumentException("message is required");
    }
    return send(destination, message, message.getDescriptorForType().getFullName(), Map.of());
  }

  /**
   * Sends a CloudEvent with an explicit CloudEvents type string.
   *
   * @param destination the target destination (e.g., Kafka topic)
   * @param message the protobuf payload
   * @param eventType CloudEvents type (e.g. {@code tech.maze.markets.sync.request})
   * @return true if the event was sent, false otherwise
   */
  public boolean send(String destination, Message message, String eventType) {
    return send(destination, message, eventType, Map.of());
  }

  /**
   * Sends a CloudEvent with an explicit type and CloudEvents extensions.
   *
   * @param destination the target destination (e.g., Kafka topic)
   * @param message the protobuf payload
   * @param eventType CloudEvents type
   * @param extensions CloudEvents extensions (replyto, correlationid, ...)
   * @return true if the event was sent, false otherwise
   */
  public boolean send(
      String destination,
      Message message,
      String eventType,
      Map<String, String> extensions
  ) {
    if (destination == null || destination.isBlank()) {
      throw new IllegalArgumentException("destination is required");
    }
    if (message == null) {
      throw new IllegalArgumentException("message is required");
    }
    if (eventType == null || eventType.isBlank()) {
      throw new IllegalArgumentException("eventType is required");
    }

    final CloudEventBuilder builder = CloudEventBuilder.v1()
        .withId(UUID.randomUUID().toString())
        .withSource(eventProperties.getSource())
        .withType(eventType)
        .withTime(OffsetDateTime.now())
        .withDataContentType(PROTO_DATA_CONTENT_TYPE)
        .withData(ProtoCloudEventData.wrap(message));

    if (extensions != null) {
      extensions.forEach((key, value) -> {
        if (key != null && !key.isBlank() && value != null && !value.isBlank()) {
          builder.withExtension(key, value);
        }
      });
    }

    final CloudEvent event = builder.build();

    return retryTemplate.execute(context -> {
      final boolean sent = streamBridge.send(destination, event);
      if (!sent) {
        throw new IllegalStateException("Failed to send event to " + destination);
      }
      return true;
    }, context -> false);
  }

  private String getExtensionAsString(CloudEvent event, String key) {
    if (event == null || key == null || key.isBlank()) {
      return null;
    }

    final Object value = event.getExtension(key);
    if (value == null) {
      return null;
    }

    return value.toString();
  }
}
