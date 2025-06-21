/*
 * Copyright 2024-2024 the original author or authors.
 */

package io.modelcontextprotocol.client.transport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A utility class that implements WebClient behavior using java.net.http.HttpClient,
 * reactor.core.publisher.Mono and reactor.core.publisher.Flux dependencies.
 *
 * This class provides a similar API to Spring WebFlux WebClient but uses the standard
 * Java HTTP client underneath without Spring dependencies.
 *
 * @author Christian Tzolov
 */
public class SimpleWebClient {

	private static final Logger logger = LoggerFactory.getLogger(SimpleWebClient.class);

	private final HttpClient httpClient;

	private final ObjectMapper objectMapper;

	private final URI baseUri;

	private SimpleWebClient(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri) {
		this.httpClient = httpClient;
		this.objectMapper = objectMapper;
		this.baseUri = baseUri;
	}

	public String getBaseUrl() {
		return baseUri != null ? baseUri.toString() : null;
	}

	public HttpClient getHttpClient() {
		return this.httpClient;
	}

	/**
	 * Create a new SimpleWebClient builder.
	 * @return a new builder instance
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Start building a POST request.
	 * @return a POST request specification
	 */
	public PostRequestSpec post() {
		return new PostRequestSpec();
	}

	/**
	 * Builder for creating SimpleWebClient instances.
	 */
	public static class Builder {

		private HttpClient httpClient = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.connectTimeout(Duration.ofSeconds(10))
			.build();

		private ObjectMapper objectMapper = new ObjectMapper();

		private URI baseUri;

		/**
		 * Configure the base URI for requests.
		 * @param baseUri the base URI
		 * @return this builder
		 */
		public Builder baseUrl(String baseUri) {
			this.baseUri = URI.create(baseUri);
			return this;
		}

		/**
		 * Configure the HttpClient to use.
		 * @param httpClient the HTTP client
		 * @return this builder
		 */
		public Builder httpClient(HttpClient httpClient) {
			this.httpClient = httpClient;
			return this;
		}

		/**
		 * Configure the ObjectMapper to use.
		 * @param objectMapper the object mapper
		 * @return this builder
		 */
		public Builder objectMapper(ObjectMapper objectMapper) {
			this.objectMapper = objectMapper;
			return this;
		}

		/**
		 * Build the SimpleWebClient instance.
		 * @return a new SimpleWebClient
		 */
		public SimpleWebClient build() {
			return new SimpleWebClient(httpClient, objectMapper, baseUri);
		}

	}

	/**
	 * Simple headers implementation.
	 */
	public static class SimpleHeaders {

		private final Map<String, List<String>> headers = new HashMap<>();

		public void add(String name, String value) {
			headers.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
		}

		public void put(String name, String value) {
			List<String> values = new ArrayList<>();
			values.add(value);
			headers.put(name, values);
		}

		public Optional<String> getFirst(String name) {
			List<String> values = headers.get(name);
			return values != null && !values.isEmpty() ? Optional.of(values.get(0)) : Optional.empty();
		}

		public List<String> getAll(String name) {
			List<String> values = headers.get(name);
			return values != null ? new ArrayList<>(values) : new ArrayList<>();
		}

		public Map<String, String> asMap() {
			Map<String, String> result = new HashMap<>();
			headers.forEach((name, values) -> {
				if (!values.isEmpty()) {
					result.put(name, values.stream().collect(Collectors.joining(", ")));
				}
			});
			return result;
		}

		public Map<String, List<String>> asMultiMap() {
			Map<String, List<String>> result = new HashMap<>();
			headers.forEach((name, values) -> result.put(name, new ArrayList<>(values)));
			return result;
		}

		public void forEach(BiConsumer<String, String> action) {
			headers.forEach((name, values) -> {
				if (!values.isEmpty()) {
					action.accept(name, values.get(0));
				}
			});
		}

		public void forEachMultiValue(BiConsumer<String, java.util.List<String>> action) {
			headers.forEach((name, values) -> action.accept(name, new ArrayList<>(values)));
		}

		/**
		 * Get content type from headers.
		 * @return optional content type
		 */
		public Optional<SimpleMediaType> contentType() {
			return getFirst("content-type").map(SimpleMediaType::new);
		}

	}

	/**
	 * Simple MediaType implementation for compatibility.
	 */
	public static class SimpleMediaType {

		private final String value;

		public SimpleMediaType(String value) {
			this.value = value;
		}

		public boolean isCompatibleWith(String mediaType) {
			return value != null && value.contains(mediaType);
		}

		public String toString() {
			return value;
		}

		public static final String TEXT_EVENT_STREAM = "text/event-stream";

		public static final String APPLICATION_JSON = "application/json";

	}

	/**
	 * Base class for request specifications.
	 */
	public abstract class BaseRequestSpec {

		protected URI uri;

		protected SimpleHeaders headers = new SimpleHeaders();

		/**
		 * Set the URI for the request.
		 * @param uri the URI
		 * @return this request spec
		 */
		public BaseRequestSpec uri(String uri) {
			this.uri = baseUri != null ? baseUri.resolve(uri) : URI.create(uri);
			return this;
		}

		/**
		 * Set headers for the request.
		 * @param headersConsumer consumer to configure headers
		 * @return this request spec
		 */
		public BaseRequestSpec headers(Consumer<SimpleHeaders> headersConsumer) {
			headersConsumer.accept(this.headers);
			return this;
		}

		/**
		 * Add an Accept header.
		 * @param mediaTypes the media types to accept
		 * @return this request spec
		 */
		public BaseRequestSpec accept(String... mediaTypes) {
			for (String mediaType : mediaTypes) {
				headers.add("Accept", mediaType);
			}
			return this;
		}

		/**
		 * Execute the request and return a Flux for Server-Sent Events.
		 * @param responseHandler function to handle the response
		 * @param <T> the response type
		 * @return a Flux of the response type
		 */
		public <T> Flux<T> exchangeToFlux(Function<SimpleClientResponse, Flux<T>> responseHandler) {
			throw new UnsupportedOperationException();
		}

		/**
		 * Execute the request and retrieve the response.
		 * @return a ResponseSpec for further processing
		 */
		public ResponseSpec retrieve() {
			throw new UnsupportedOperationException();
		}

		protected HttpRequest.Builder createBaseRequest() {
			HttpRequest.Builder builder = HttpRequest.newBuilder().uri(uri).timeout(Duration.ofSeconds(30));

			// Add headers - support multiple values per header name
			headers.forEachMultiValue((name, values) -> {
				for (String value : values) {
					builder.header(name, value);
				}
			});

			return builder;
		}

	}

	/**
	 * POST request specification.
	 */
	public class PostRequestSpec extends BaseRequestSpec {

		private Object body;

		/**
		 * Set the request body.
		 * @param body the request body
		 * @return this request spec
		 */
		public PostRequestSpec bodyValue(Object body) {
			this.body = body;
			return this;
		}

		/**
		 * Execute the POST request and return a Flux.
		 * @param responseHandler function to handle the response
		 * @param <T> the response type
		 * @return a Flux of the response type
		 */
		public <T> Flux<T> exchangeToFlux(Function<SimpleClientResponse, Flux<T>> responseHandler) {
			try {
				String jsonBody = objectMapper.writeValueAsString(body);

				HttpRequest request = createBaseRequest().header("Content-Type", "application/json")
					.POST(HttpRequest.BodyPublishers.ofString(jsonBody))
					.build();

				return Mono.fromFuture(() -> httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()))
					.map(SimpleClientResponse::new)
					.flatMapMany(responseHandler)
					.onErrorMap(this::mapException);
			}
			catch (IOException e) {
				return Flux.error(new RuntimeException("Failed to serialize request body", e));
			}
		}

		private Throwable mapException(Throwable throwable) {
			if (throwable instanceof RuntimeException) {
				return throwable;
			}
			return new RuntimeException("HTTP request failed", throwable);
		}

	}

	/**
	 * Response specification for handling responses.
	 */
	public static class ResponseSpec {

		private final Mono<SimpleClientResponse> responseMono;

		public ResponseSpec(Mono<SimpleClientResponse> responseMono) {
			this.responseMono = responseMono;
		}

		/**
		 * Convert the response to a bodiless entity.
		 * @return a Mono of Void
		 */
		public Mono<Void> toBodilessEntity() {
			return responseMono.then();
		}

	}

	/**
	 * Simple Server-Sent Event implementation.
	 */
	public static class SimpleServerSentEvent<T> {

		private final T data;

		private final String event;

		private final String id;

		public SimpleServerSentEvent(T data, String event, String id) {
			this.data = data;
			this.event = event;
			this.id = id;
		}

		public T data() {
			return data;
		}

		public String event() {
			return event;
		}

		public String id() {
			return id;
		}

		public static <T> Builder<T> builder() {
			return new Builder<>();
		}

		public static class Builder<T> {

			private T data;

			private String event;

			private String id;

			public Builder<T> data(T data) {
				this.data = data;
				return this;
			}

			public Builder<T> event(String event) {
				this.event = event;
				return this;
			}

			public Builder<T> id(String id) {
				this.id = id;
				return this;
			}

			public SimpleServerSentEvent<T> build() {
				return new SimpleServerSentEvent<>(data, event, id);
			}

		}

	}

	/**
	 * Simple implementation of ClientResponse that wraps HttpResponse.
	 */
	public static class SimpleClientResponse {

		private final HttpResponse<String> httpResponse;

		private final SimpleHeaders headers;

		public SimpleClientResponse(HttpResponse<String> httpResponse) {
			this.httpResponse = httpResponse;
			this.headers = new SimpleHeaders();

			// Convert HttpResponse headers to SimpleHeaders - preserve all values
			httpResponse.headers().map().forEach((name, values) -> {
				for (String value : values) {
					headers.add(name, value);
				}
			});
		}

		/**
		 * Get the HTTP status code.
		 * @return the HTTP status code
		 */
		public int statusCode() {
			return httpResponse.statusCode();
		}

		/**
		 * Get the response headers.
		 * @return the headers
		 */
		public SimpleHeaders headers() {
			return headers;
		}

		/**
		 * Check if the response status is 2xx successful.
		 * @return true if successful
		 */
		public boolean is2xxSuccessful() {
			int status = statusCode();
			return status >= 200 && status < 300;
		}

		/**
		 * Get content type from headers as a simple MediaType-like object.
		 * @return optional content type
		 */
		public Optional<SimpleMediaType> contentType() {
			return headers.getFirst("content-type").map(SimpleMediaType::new);
		}

		/**
		 * Convert the response body to a Flux of Server-Sent Events.
		 * @param typeRef the type reference (for compatibility)
		 * @param <T> the type parameter
		 * @return a Flux of Server-Sent Events
		 */
		public <T> Flux<T> bodyToFlux(TypeReference<T> typeRef) {
			String body = httpResponse.body();
			if (body == null || body.isEmpty()) {
				return Flux.empty();
			}

			// Parse Server-Sent Events from the response body
			return Flux.fromArray(body.split("\n\n"))
				.filter(chunk -> !chunk.trim().isEmpty())
				.map(chunk -> parseSseEvent(chunk, typeRef))
				.filter(Optional::isPresent)
				.map(Optional::get);
		}

		/**
		 * Convert the response body to a Mono of String.
		 * @param clazz the class type
		 * @return a Mono of the response body
		 */
		public Mono<String> bodyToMono(Class<String> clazz) {
			return Mono.justOrEmpty(httpResponse.body());
		}

		/**
		 * Create an error from the response.
		 * @param <T> the type parameter
		 * @return a Mono error
		 */
		public <T> Mono<T> createError() {
			String message = "HTTP " + httpResponse.statusCode();
			String body = httpResponse.body();
			if (body != null && !body.isEmpty()) {
				message += ": " + body;
			}
			return Mono.error(new RuntimeException(message));
		}

		@SuppressWarnings("unchecked")
		private <T> Optional<T> parseSseEvent(String chunk, TypeReference<T> typeRef) {
			try {
				String[] lines = chunk.split("\n");
				String eventType = null;
				String data = null;
				String id = null;

				for (String line : lines) {
					if (line.startsWith("event:")) {
						eventType = line.substring(6).trim();
					}
					else if (line.startsWith("data:")) {
						data = line.substring(5).trim();
					}
					else if (line.startsWith("id:")) {
						id = line.substring(3).trim();
					}
				}

				if (data != null) {
					// Create a SimpleServerSentEvent
					SimpleServerSentEvent<String> event = SimpleServerSentEvent.<String>builder()
						.data(data)
						.event(eventType)
						.id(id)
						.build();

					return Optional.of((T) event);
				}
			}
			catch (Exception e) {
				logger.warn("Failed to parse SSE event: {}", chunk, e);
			}
			return Optional.empty();
		}

	}

}
