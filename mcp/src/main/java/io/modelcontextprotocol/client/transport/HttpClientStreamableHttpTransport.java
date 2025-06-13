/*
 * Copyright 2024-2024 the original author or authors.
 */

package io.modelcontextprotocol.client.transport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.modelcontextprotocol.client.transport.FlowSseClient.LineSubscriber;
import io.modelcontextprotocol.client.transport.FlowSseClient.SseEvent;
import io.modelcontextprotocol.client.transport.SimpleWebClient.SimpleClientResponse;
import io.modelcontextprotocol.client.transport.SimpleWebClient.SimpleMediaType;
import io.modelcontextprotocol.client.transport.SimpleWebClient.SimpleServerSentEvent;
import io.modelcontextprotocol.spec.DefaultMcpTransportSession;
import io.modelcontextprotocol.spec.DefaultMcpTransportStream;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpTransportSession;
import io.modelcontextprotocol.spec.McpTransportSessionNotFoundException;
import io.modelcontextprotocol.spec.McpTransportStream;
import io.modelcontextprotocol.util.Assert;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * An implementation of the Streamable HTTP protocol as defined by the
 * <code>2025-03-26</code> version of the MCP specification.
 *
 * <p>
 * The transport is capable of resumability and reconnects. It reacts to transport-level
 * session invalidation and will propagate {@link McpTransportSessionNotFoundException
 * appropriate exceptions} to the higher level abstraction layer when needed in order to
 * allow proper state management. The implementation handles servers that are stateful and
 * provide session meta information, but can also communicate with stateless servers that
 * do not provide a session identifier and do not support SSE streams.
 * </p>
 * <p>
 * This implementation does not handle backwards compatibility with the <a href=
 * "https://modelcontextprotocol.io/specification/2024-11-05/basic/transports#http-with-sse">"HTTP
 * with SSE" transport</a>. In order to communicate over the phased-out
 * <code>2024-11-05</code> protocol, use {@link HttpClientSseClientTransport} or
 * {@link WebFluxSseClientTransport}.
 * </p>
 *
 * @author Dariusz Jędrzejczyk
 * @author Christian Tzolov
 * @see <a href=
 * "https://modelcontextprotocol.io/specification/2025-03-26/basic/transports#streamable-http">Streamable
 * HTTP transport specification</a>
 */
public class HttpClientStreamableHttpTransport implements McpClientTransport {

	private static final Logger logger = LoggerFactory.getLogger(HttpClientStreamableHttpTransport.class);

	private static final String DEFAULT_ENDPOINT = "/mcp";

	/**
	 * Event type for JSON-RPC messages received through the SSE connection. The server
	 * sends messages with this event type to transmit JSON-RPC protocol data.
	 */
	private static final String MESSAGE_EVENT_TYPE = "message";

	private static final String APPLICATION_JSON = "application/json";

	private static final String TEXT_EVENT_STREAM = "text/event-stream";

	public static int NOT_FOUND = 404;

	public static int METHOD_NOT_ALLOWED = 405;

	private static final TypeReference<SimpleServerSentEvent<String>> PARAMETERIZED_TYPE_REF = new TypeReference<>() {
	};

	private final ObjectMapper objectMapper;

	private final SimpleWebClient webClient;

	private final String endpoint;

	private final boolean openConnectionOnStartup;

	private final boolean resumableStreams;

	private final AtomicReference<DefaultMcpTransportSession> activeSession = new AtomicReference<>();

	private final AtomicReference<Function<Mono<McpSchema.JSONRPCMessage>, Mono<McpSchema.JSONRPCMessage>>> handler = new AtomicReference<>();

	private final AtomicReference<Consumer<Throwable>> exceptionHandler = new AtomicReference<>();

	private HttpClientStreamableHttpTransport(ObjectMapper objectMapper, SimpleWebClient.Builder webClientBuilder,
			String endpoint, boolean resumableStreams, boolean openConnectionOnStartup) {
		this.objectMapper = objectMapper;
		this.webClient = webClientBuilder.build();
		this.endpoint = endpoint;
		this.resumableStreams = resumableStreams;
		this.openConnectionOnStartup = openConnectionOnStartup;
		this.activeSession.set(createTransportSession());
	}

	/**
	 * Create a stateful builder for creating {@link HttpClientStreamableHttpTransport}
	 * instances.
	 * @param webClientBuilder the {@link WebClient.Builder} to use
	 * @return a builder which will create an instance of
	 * {@link HttpClientStreamableHttpTransport} once {@link Builder#build()} is called
	 */
	public static Builder builder(SimpleWebClient.Builder webClientBuilder) {
		return new Builder(webClientBuilder);
	}

	@Override
	public Mono<Void> connect(Function<Mono<McpSchema.JSONRPCMessage>, Mono<McpSchema.JSONRPCMessage>> handler) {
		return Mono.deferContextual(ctx -> {
			this.handler.set(handler);
			if (openConnectionOnStartup) {
				logger.debug("Eagerly opening connection on startup");
				return this.reconnect(null).then();
			}
			return Mono.empty();
		});
	}

	private DefaultMcpTransportSession createTransportSession() {
		Function<String, Publisher<Void>> onClose = sessionId -> sessionId == null ? Mono.empty()
				: webClient.delete().uri(this.endpoint).headers(httpHeaders -> {
					httpHeaders.add("mcp-session-id", sessionId);
				})
					.retrieve()
					.toBodilessEntity()
					.doOnError(e -> logger.warn("Got error when closing transport", e))
					.then();
		return new DefaultMcpTransportSession(onClose);
	}

	@Override
	public void setExceptionHandler(Consumer<Throwable> handler) {
		logger.debug("Exception handler registered");
		this.exceptionHandler.set(handler);
	}

	private void handleException(Throwable t) {
		logger.debug("Handling exception for session {}", sessionIdOrPlaceholder(this.activeSession.get()), t);
		if (t instanceof McpTransportSessionNotFoundException) {
			McpTransportSession<?> invalidSession = this.activeSession.getAndSet(createTransportSession());
			logger.warn("Server does not recognize session {}. Invalidating.", invalidSession.sessionId());
			invalidSession.close();
		}
		Consumer<Throwable> handler = this.exceptionHandler.get();
		if (handler != null) {
			handler.accept(t);
		}
	}

	@Override
	public Mono<Void> closeGracefully() {
		return Mono.defer(() -> {
			logger.debug("Graceful close triggered");
			DefaultMcpTransportSession currentSession = this.activeSession.getAndSet(createTransportSession());
			if (currentSession != null) {
				return currentSession.closeGracefully();
			}
			return Mono.empty();
		});
	}

	private Mono<Disposable> reconnect(McpTransportStream<Disposable> stream) {
		return Mono.deferContextual(ctx -> {

			// TODO: fix
			HttpClient httpClient = this.webClient.getHttpClient();

			// TDOO: extract the request builder as a configurable field
			HttpRequest.Builder requestBuilder = HttpRequest.newBuilder();

			if (stream != null) {
				logger.debug("Reconnecting stream {} with lastId {}", stream.streamId(), stream.lastId());
			}
			else {
				logger.debug("Reconnecting with no prior stream");
			}

			final AtomicReference<Disposable> disposableRef = new AtomicReference<>();
			final McpTransportSession<Disposable> transportSession = this.activeSession.get();

			if (transportSession != null && transportSession.sessionId().isPresent()) {
				requestBuilder = requestBuilder.header("mcp-session-id", transportSession.sessionId().get());
			}

			if (stream != null && stream.lastId().isPresent()) {
				requestBuilder = requestBuilder.header("last-event-id", stream.lastId().get());
			}

			HttpRequest request = requestBuilder.uri(URI.create(this.endpoint))
				.header("Accept", TEXT_EVENT_STREAM)
				.header("Cache-Control", "no-cache")
				.GET()
				.build();

			Disposable connection = Flux.<SseEvent>create(
					sseSink -> httpClient.sendAsync(request, responseInfo -> toBodySubscriber(responseInfo, sseSink))
						.whenComplete((response, throwable) -> {
							if (throwable != null) {
								sseSink.error(throwable);
							}
							else {
								int status = response.statusCode();

								if (status == METHOD_NOT_ALLOWED) { // NotAllowed
									logger
										.debug("The server does not support SSE streams, using request-response mode.");
									sseSink.complete();
								}
								else if (status == NOT_FOUND) { // NotFound
									String sessionIdRepresentation = sessionIdOrPlaceholder(transportSession);
									sseSink.error(new McpTransportSessionNotFoundException(
											"Session not found for session ID: " + sessionIdRepresentation));
								}
								else if (!isEventStream(response)) {
									String message = "Failed to connect to SSE stream. HTTP " + response.statusCode();
									if (response.body() != null) {
										message += ": " + response.body();
									}
									logger.info("Opening an SSE stream failed. This can be safely ignored." + message);
									sseSink.error(new RuntimeException(message));
								}
								// If status is OK, the lineSubscriber will handle the
								// stream
								logger.debug("Established SSE stream via GET");
							}
						}))
				.flatMap(sse -> {
					if (MESSAGE_EVENT_TYPE.equals(sse.event())) {
						try {
							// We don't support batching ATM and probably won't since the
							// next version considers removing it.
							McpSchema.JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(this.objectMapper,
									sse.data());

							Tuple2<Optional<String>, Iterable<McpSchema.JSONRPCMessage>> idWithMessages = Tuples
								.of(Optional.ofNullable(sse.id()), List.of(message));

							McpTransportStream<Disposable> sessionStream = stream != null ? stream
									: new DefaultMcpTransportStream<>(this.resumableStreams, this::reconnect);
							logger.debug("Connected stream {}", sessionStream.streamId());

							return Flux.from(sessionStream.consumeSseStream(Flux.just(idWithMessages)));

						}
						catch (IOException ioException) {
							return Flux.<McpSchema.JSONRPCMessage>error(
									new McpError("Error parsing JSON-RPC message: " + sse.data()));
						}
					}

					return Flux.<McpSchema.JSONRPCMessage>error(
							new McpError("Received unrecognized SSE event type: " + sse.event()));

				}).<McpSchema
						.JSONRPCMessage>flatMap(jsonrpcMessage -> this.handler.get().apply(Mono.just(jsonrpcMessage)))
				.onErrorComplete(t -> {
					this.handleException(t);
					return true;
				})
				.doFinally(s -> {
					Disposable ref = disposableRef.getAndSet(null);
					if (ref != null) {
						transportSession.removeConnection(ref);
					}
				})
				.contextWrite(ctx)
				.subscribe();

			disposableRef.set(connection);
			transportSession.addConnection(connection);
			return Mono.just(connection);
		});
	}

	private static boolean isEventStream(HttpResponse<Void> response) {
		String contentType = response.headers().firstValue("Content-Type").orElse("").toLowerCase();
		return response.statusCode() >= 200 && response.statusCode() < 300 && contentType.contains(TEXT_EVENT_STREAM);
	}

	private BodySubscriber<Void> toBodySubscriber(ResponseInfo responseInfo, FluxSink<SseEvent> sseSink) {
		return HttpResponse.BodySubscribers
			.fromLineSubscriber(FlowAdapters.toFlowSubscriber(new LineSubscriber(sseSink)));
	}

	@Override
	public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message) {
		return Mono.create(sink -> {
			logger.debug("Sending message {}", message);
			// Here we attempt to initialize the client.
			// In case the server supports SSE, we will establish a long-running session
			// here and listen for messages.
			// If it doesn't, nothing actually happens here, that's just the way it is...
			final AtomicReference<Disposable> disposableRef = new AtomicReference<>();
			final McpTransportSession<Disposable> transportSession = this.activeSession.get();

			Disposable connection = webClient.post()
				.bodyValue(message)
				.uri(this.endpoint)
				.accept(TEXT_EVENT_STREAM, APPLICATION_JSON)
				.headers(httpHeaders -> {
					transportSession.sessionId().ifPresent(id -> httpHeaders.add("mcp-session-id", id));
				}).<McpSchema.JSONRPCMessage>exchangeToFlux(response -> {
					if (transportSession
						.markInitialized(response.headers().getFirst("mcp-session-id").orElseGet(() -> null))) {
						// Once we have a session, we try to open an async stream for
						// the server to send notifications and requests out-of-band.
						reconnect(null).contextWrite(sink.contextView()).subscribe();
					}

					String sessionRepresentation = sessionIdOrPlaceholder(transportSession);

					// The spec mentions only ACCEPTED, but the existing SDKs can return
					// 200 OK for notifications
					if (response.is2xxSuccessful()) {
						Optional<SimpleMediaType> contentType = response.headers().contentType();
						// Existing SDKs consume notifications with no response body nor
						// content type
						if (contentType.isEmpty()) {
							logger.trace("Message was successfully sent via POST for session {}",
									sessionRepresentation);
							// signal the caller that the message was successfully
							// delivered
							sink.success();
							// communicate to downstream there is no streamed data coming
							return Flux.empty();
						}
						else {
							SimpleMediaType mediaType = contentType.get();
							if (mediaType.isCompatibleWith(SimpleMediaType.TEXT_EVENT_STREAM)) {
								logger.debug("Established SSE stream via POST");
								// communicate to caller that the message was delivered
								sink.success();
								// starting a stream
								return newEventStream(response, sessionRepresentation);
							}
							else if (mediaType.isCompatibleWith(SimpleMediaType.APPLICATION_JSON)) {
								logger.trace("Received response to POST for session {}", sessionRepresentation);
								// communicate to caller the message was delivered
								sink.success();
								return responseFlux(response);
							}
							else {
								logger.warn("Unknown media type {} returned for POST in session {}", contentType,
										sessionRepresentation);
								return Flux.<McpSchema.JSONRPCMessage>error(
										new RuntimeException("Unknown media type returned: " + contentType));
							}
						}
					}
					else {
						if (isNotFound(response)) {
							return mcpSessionNotFoundError(sessionRepresentation);
						}
						return extractError(response, sessionRepresentation);
					}
				})
				.flatMap(jsonRpcMessage -> this.handler.get().apply(Mono.just(jsonRpcMessage)))
				.onErrorResume(t -> {
					// handle the error first
					this.handleException(t);
					// inform the caller of sendMessage
					sink.error(t);
					return Flux.empty();
				})
				.doFinally(s -> {
					Disposable ref = disposableRef.getAndSet(null);
					if (ref != null) {
						transportSession.removeConnection(ref);
					}
				})
				.contextWrite(sink.contextView())
				.subscribe();
			disposableRef.set(connection);
			transportSession.addConnection(connection);
		});
	}

	private static Flux<McpSchema.JSONRPCMessage> mcpSessionNotFoundError(String sessionRepresentation) {
		logger.warn("Session {} was not found on the MCP server", sessionRepresentation);
		// inform the stream/connection subscriber
		return Flux.error(new McpTransportSessionNotFoundException(sessionRepresentation));
	}

	private Flux<McpSchema.JSONRPCMessage> extractError(SimpleClientResponse response, String sessionRepresentation) {
		return response.<McpSchema.JSONRPCMessage>createError().onErrorResume(e -> {
			// Since we're using FakeWebClient, we don't have WebClientResponseException
			// We'll work with the response body directly
			String body = response.bodyToMono(String.class).block();
			McpSchema.JSONRPCResponse.JSONRPCError jsonRpcError = null;
			Exception toPropagate;
			try {
				if (body != null && !body.isEmpty()) {
					McpSchema.JSONRPCResponse jsonRpcResponse = objectMapper.readValue(body,
							McpSchema.JSONRPCResponse.class);
					jsonRpcError = jsonRpcResponse.error();
					toPropagate = new McpError(jsonRpcError);
				}
				else {
					toPropagate = new RuntimeException("Sending request failed", e);
				}
			}
			catch (IOException ex) {
				toPropagate = new RuntimeException("Sending request failed", e);
				logger.debug("Received content together with {} HTTP code response: {}", response.statusCode(), body);
			}

			// Some implementations can return 400 when presented with a
			// session id that it doesn't know about, so we will
			// invalidate the session
			// https://github.com/modelcontextprotocol/typescript-sdk/issues/389
			if (response.statusCode() == 400) {
				return Mono.error(new McpTransportSessionNotFoundException(sessionRepresentation, toPropagate));
			}
			return Mono.empty();
		}).flux();
	}

	private Flux<McpSchema.JSONRPCMessage> eventStream(McpTransportStream<Disposable> stream,
			SimpleClientResponse response) {
		McpTransportStream<Disposable> sessionStream = stream != null ? stream
				: new DefaultMcpTransportStream<>(this.resumableStreams, this::reconnect);
		logger.debug("Connected stream {}", sessionStream.streamId());

		Flux<Tuple2<Optional<String>, Iterable<McpSchema.JSONRPCMessage>>> idWithMessages = response
			.bodyToFlux(PARAMETERIZED_TYPE_REF)
			.map(this::parse);

		return Flux.from(sessionStream.consumeSseStream(idWithMessages));
	}

	private static boolean isNotFound(SimpleClientResponse response) {
		return response.statusCode() == NOT_FOUND;
	}

	private static String sessionIdOrPlaceholder(McpTransportSession<?> transportSession) {
		return transportSession.sessionId().orElse("[missing_session_id]");
	}

	private Flux<McpSchema.JSONRPCMessage> responseFlux(SimpleClientResponse response) {
		return response.bodyToMono(String.class).<Iterable<McpSchema.JSONRPCMessage>>handle((responseMessage, s) -> {
			try {
				McpSchema.JSONRPCMessage jsonRpcResponse = McpSchema.deserializeJsonRpcMessage(objectMapper,
						responseMessage);
				s.next(List.of(jsonRpcResponse));
			}
			catch (IOException e) {
				s.error(e);
			}
		}).flatMapIterable(Function.identity());
	}

	private Flux<McpSchema.JSONRPCMessage> newEventStream(SimpleClientResponse response, String sessionRepresentation) {
		McpTransportStream<Disposable> sessionStream = new DefaultMcpTransportStream<>(this.resumableStreams,
				this::reconnect);
		logger.trace("Sent POST and opened a stream ({}) for session {}", sessionStream.streamId(),
				sessionRepresentation);
		return eventStream(sessionStream, response);
	}

	@Override
	public <T> T unmarshalFrom(Object data, TypeReference<T> typeRef) {
		return this.objectMapper.convertValue(data, typeRef);
	}

	private Tuple2<Optional<String>, Iterable<McpSchema.JSONRPCMessage>> parse(SimpleServerSentEvent<String> event) {
		if (MESSAGE_EVENT_TYPE.equals(event.event())) {
			try {
				// We don't support batching ATM and probably won't since the next version
				// considers removing it.
				McpSchema.JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(this.objectMapper, event.data());
				return Tuples.of(Optional.ofNullable(event.id()), List.of(message));
			}
			catch (IOException ioException) {
				throw new McpError("Error parsing JSON-RPC message: " + event.data());
			}
		}
		else {
			throw new McpError("Received unrecognized SSE event type: " + event.event());
		}
	}

	/**
	 * Builder for {@link HttpClientStreamableHttpTransport}.
	 */
	public static class Builder {

		private ObjectMapper objectMapper;

		private SimpleWebClient.Builder webClientBuilder;

		private String endpoint = DEFAULT_ENDPOINT;

		private boolean resumableStreams = true;

		private boolean openConnectionOnStartup = false;

		private Builder(SimpleWebClient.Builder webClientBuilder) {
			Assert.notNull(webClientBuilder, "WebClient.Builder must not be null");
			this.webClientBuilder = webClientBuilder;
		}

		/**
		 * Configure the {@link ObjectMapper} to use.
		 * @param objectMapper instance to use
		 * @return the builder instance
		 */
		public Builder objectMapper(ObjectMapper objectMapper) {
			Assert.notNull(objectMapper, "ObjectMapper must not be null");
			this.objectMapper = objectMapper;
			return this;
		}

		/**
		 * Configure the {@link WebClient.Builder} to construct the {@link WebClient}.
		 * @param webClientBuilder instance to use
		 * @return the builder instance
		 */
		public Builder webClientBuilder(SimpleWebClient.Builder webClientBuilder) {
			Assert.notNull(webClientBuilder, "WebClient.Builder must not be null");
			this.webClientBuilder = webClientBuilder;
			return this;
		}

		/**
		 * Configure the endpoint to make HTTP requests against.
		 * @param endpoint endpoint to use
		 * @return the builder instance
		 */
		public Builder endpoint(String endpoint) {
			Assert.hasText(endpoint, "endpoint must be a non-empty String");
			this.endpoint = endpoint;
			return this;
		}

		/**
		 * Configure whether to use the stream resumability feature by keeping track of
		 * SSE event ids.
		 * @param resumableStreams if {@code true} event ids will be tracked and upon
		 * disconnection, the last seen id will be used upon reconnection as a header to
		 * resume consuming messages.
		 * @return the builder instance
		 */
		public Builder resumableStreams(boolean resumableStreams) {
			this.resumableStreams = resumableStreams;
			return this;
		}

		/**
		 * Configure whether the client should open an SSE connection upon startup. Not
		 * all servers support this (although it is in theory possible with the current
		 * specification), so use with caution. By default, this value is {@code false}.
		 * @param openConnectionOnStartup if {@code true} the {@link #connect(Function)}
		 * method call will try to open an SSE connection before sending any JSON-RPC
		 * request
		 * @return the builder instance
		 */
		public Builder openConnectionOnStartup(boolean openConnectionOnStartup) {
			this.openConnectionOnStartup = openConnectionOnStartup;
			return this;
		}

		/**
		 * Construct a fresh instance of {@link HttpClientStreamableHttpTransport} using
		 * the current builder configuration.
		 * @return a new instance of {@link HttpClientStreamableHttpTransport}
		 */
		public HttpClientStreamableHttpTransport build() {
			ObjectMapper objectMapper = this.objectMapper != null ? this.objectMapper : new ObjectMapper();

			return new HttpClientStreamableHttpTransport(objectMapper, this.webClientBuilder, endpoint,
					resumableStreams, openConnectionOnStartup);
		}

	}

}
