/*
* Copyright 2024 - 2024 the original author or authors.
*/
package io.modelcontextprotocol.client.transport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscriber;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Subscription;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * A Server-Sent Events (SSE) client using Java's Flow API for reactive stream processing.
 * Connects to SSE endpoints and parses incoming events according to the W3C SSE
 * specification.
 *
 * <p>
 * Supports standard SSE fields: event type, id, and data (including multi-line). Events
 * are delivered as a {@link Flux} of {@link SseEvent} records.
 *
 * <p>
 * This class is thread-safe. Errors are propagated through the reactive stream.
 *
 * @author Christian Tzolov
 * @author Dariusz Jędrzejczyk
 * @see SseEvent
 * @see HttpClient
 * @see Flux
 */
public class FlowSseClient {

	/**
	 * The HTTP client used for establishing SSE connections.
	 */
	private final HttpClient httpClient;

	/**
	 * The HTTP request builder template for creating SSE requests.
	 */
	private final HttpRequest.Builder requestBuilder;

	/**
	 * Pattern to extract data content from SSE "data:" lines.
	 */
	private static final Pattern EVENT_DATA_PATTERN = Pattern.compile("^data:(.+)$", Pattern.MULTILINE);

	/**
	 * Pattern to extract event ID from SSE "id:" lines.
	 */
	private static final Pattern EVENT_ID_PATTERN = Pattern.compile("^id:(.+)$", Pattern.MULTILINE);

	/**
	 * Pattern to extract event type from SSE "event:" lines.
	 */
	private static final Pattern EVENT_TYPE_PATTERN = Pattern.compile("^event:(.+)$", Pattern.MULTILINE);

	/**
	 * Represents a Server-Sent Event with its standard fields.
	 *
	 * @param id the event ID, may be {@code null}
	 * @param event the event type, may be {@code null} (defaults to "message")
	 * @param data the event payload data, never {@code null}
	 */
	public static record SseEvent(String id, String event, String data) {
	}

	/**
	 * Creates a new FlowSseClient with the specified HTTP client using default request
	 * settings.
	 * @param httpClient the {@link HttpClient} instance to use for SSE connections
	 */
	public FlowSseClient(HttpClient httpClient) {
		this(httpClient, HttpRequest.newBuilder());
	}

	/**
	 * Creates a new FlowSseClient with the specified HTTP client and custom request
	 * builder.
	 *
	 * <p>
	 * The builder can be pre-configured with headers, authentication, or other request
	 * properties. Required SSE headers will be added automatically.
	 * @param httpClient the {@link HttpClient} instance to use for SSE connections
	 * @param requestBuilder the {@link HttpRequest.Builder} template to use for SSE
	 * requests
	 */
	public FlowSseClient(HttpClient httpClient, HttpRequest.Builder requestBuilder) {
		this.httpClient = httpClient;
		this.requestBuilder = requestBuilder;
	}

	/**
	 * Subscribes to an SSE endpoint and returns a reactive stream of events.
	 *
	 * <p>
	 * The returned {@link Flux} is cold - each subscription creates a separate HTTP
	 * connection. Required SSE headers are set automatically. Events are parsed according
	 * to the SSE specification.
	 * @param url the SSE endpoint URL to connect to
	 * @return a {@link Flux} of {@link SseEvent} objects representing the event stream
	 * @throws IllegalArgumentException if the URL is malformed
	 * @throws RuntimeException if the connection fails with an unexpected HTTP status
	 * code
	 */
	public Flux<SseEvent> events(String url) {

		HttpRequest request = this.requestBuilder.uri(URI.create(url))
			.header("Accept", "text/event-stream")
			.header("Cache-Control", "no-cache")
			.GET()
			.build();

		return Flux.<SseEvent>create(sseSink -> httpClient.sendAsync(request, responseInfo -> toBodySubscriber(sseSink))
			.whenComplete((response, throwable) -> {
				if (throwable != null) {
					sseSink.error(throwable);
				}
				else {
					int status = response.statusCode();
					if (status != 200 && status != 201 && status != 202 && status != 204 && status != 206) {
						sseSink.error(new RuntimeException(
								"Failed to connect to SSE stream. Unexpected status code: " + status));
					}
					// If status is OK, the lineSubscriber will handle the stream
				}
			}));
	}

	/**
	 * Creates a {@link BodySubscriber} that processes the HTTP response body as SSE
	 * events.
	 * @param sseSink the {@link FluxSink} to emit parsed {@link SseEvent} objects to
	 * @return a {@link BodySubscriber} that processes SSE response bodies
	 */
	private BodySubscriber<Void> toBodySubscriber(FluxSink<SseEvent> sseSink) {
		return HttpResponse.BodySubscribers
			.fromLineSubscriber(FlowAdapters.toFlowSubscriber(new LineSubscriber(sseSink)));
	}

	/**
	 * A reactive subscriber that processes SSE protocol lines and reconstructs complete
	 * events.
	 *
	 * <p>
	 * Handles line-by-line processing of SSE data, maintaining state for the current
	 * event and emitting complete {@link SseEvent} objects when event boundaries (empty
	 * lines) are encountered.
	 */
	public static class LineSubscriber extends BaseSubscriber<String> {

		/**
		 * The sink for emitting parsed SSE events.
		 */
		private final FluxSink<SseEvent> sseSink;

		/**
		 * StringBuilder for accumulating multi-line event data.
		 */
		private final StringBuilder eventBuilder;

		/**
		 * Current event's ID, if specified.
		 */
		private final AtomicReference<String> currentEventId;

		/**
		 * Current event's type, if specified.
		 */
		private final AtomicReference<String> currentEventType;

		/**
		 * Creates a new LineSubscriber that will emit parsed SSE events to the provided
		 * sink.
		 * @param sseSink the {@link FluxSink} to emit parsed {@link SseEvent} objects to
		 */
		public LineSubscriber(FluxSink<SseEvent> sseSink) {
			this.sseSink = sseSink;
			this.eventBuilder = new StringBuilder();
			this.currentEventId = new AtomicReference<>();
			this.currentEventType = new AtomicReference<>();
		}

		/**
		 * Initializes the subscription and sets up disposal callback.
		 * @param subscription the {@link Subscription} to the upstream line source
		 */
		@Override
		protected void hookOnSubscribe(Subscription subscription) {

			sseSink.onRequest(n -> {
				if (subscription != null) {
					subscription.request(n);
				}
			});

			// Register disposal callback to cancel subscription when Flux is disposed
			sseSink.onDispose(() -> {
				if (subscription != null) {
					subscription.cancel();
				}
			});
		}

		/**
		 * Processes each line from the SSE stream according to the SSE protocol. Empty
		 * lines trigger event emission, other lines are parsed for data, id, or event
		 * type.
		 * @param line the line to process from the SSE stream
		 */
		@Override
		protected void hookOnNext(String line) {
			if (line.isEmpty()) {
				// Empty line means end of event
				if (this.eventBuilder.length() > 0) {
					String eventData = this.eventBuilder.toString();
					SseEvent event = new SseEvent(currentEventId.get(), currentEventType.get(), eventData.trim());

					this.sseSink.next(event);
					this.eventBuilder.setLength(0);
				}
			}
			else {
				if (line.startsWith("data:")) {
					var matcher = EVENT_DATA_PATTERN.matcher(line);
					if (matcher.find()) {
						this.eventBuilder.append(matcher.group(1).trim()).append("\n");
					}
				}
				else if (line.startsWith("id:")) {
					var matcher = EVENT_ID_PATTERN.matcher(line);
					if (matcher.find()) {
						this.currentEventId.set(matcher.group(1).trim());
					}
				}
				else if (line.startsWith("event:")) {
					var matcher = EVENT_TYPE_PATTERN.matcher(line);
					if (matcher.find()) {
						this.currentEventType.set(matcher.group(1).trim());
					}
				}
			}
		}

		/**
		 * Called when the upstream line source completes normally.
		 */
		@Override
		protected void hookOnComplete() {
			if (this.eventBuilder.length() > 0) {
				String eventData = this.eventBuilder.toString();
				SseEvent event = new SseEvent(currentEventId.get(), currentEventType.get(), eventData.trim());
				this.sseSink.next(event);
			}
			this.sseSink.complete();
		}

		/**
		 * Called when an error occurs in the upstream line source.
		 * @param throwable the error that occurred
		 */
		@Override
		protected void hookOnError(Throwable throwable) {
			this.sseSink.error(throwable);
		}

	}

}
