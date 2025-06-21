package io.modelcontextprotocol.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.SimpleWebClient;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Timeout(1500)
public class HttpClientStreamableHttpAsyncClientTests extends AbstractMcpAsyncClientTests {

	private String host = "http://localhost:3001";

	// Uses the https://github.com/tzolov/mcp-everything-server-docker-image
	@SuppressWarnings("resource")
	GenericContainer<?> container = new GenericContainer<>("docker.io/tzolov/mcp-everything-server:v2")
		.withCommand("node dist/index.js streamableHttp")
		.withLogConsumer(outputFrame -> System.out.println(outputFrame.getUtf8String()))
		.withExposedPorts(3001)
		.waitingFor(Wait.forHttp("/").forStatusCode(404));

	@Override
	protected McpClientTransport createMcpTransport() {

		return HttpClientStreamableHttpTransport.builder(SimpleWebClient.builder().baseUrl(host)).build();
	}

	@Override
	protected void onStart() {
		container.start();
		int port = container.getMappedPort(3001);
		host = "http://" + container.getHost() + ":" + port;
	}

	@Override
	public void onClose() {
		container.stop();
	}

	@Test
	void testSampling2() {
		McpClientTransport transport = createMcpTransport();

		final String message = "Hello, world!";
		final String response = "Goodbye, world!";
		final int maxTokens = 100;

		AtomicReference<String> receivedPrompt = new AtomicReference<>();
		AtomicReference<String> receivedMessage = new AtomicReference<>();
		AtomicInteger receivedMaxTokens = new AtomicInteger();

		withClient(transport, spec -> spec.capabilities(McpSchema.ClientCapabilities.builder().sampling().build())
			.sampling(request -> {
				McpSchema.TextContent messageText = assertInstanceOf(McpSchema.TextContent.class,
						request.messages().get(0).content());
				receivedPrompt.set(request.systemPrompt());
				receivedMessage.set(messageText.text());
				receivedMaxTokens.set(request.maxTokens());

				return Mono
					.just(new McpSchema.CreateMessageResult(McpSchema.Role.USER, new McpSchema.TextContent(response),
							"modelId", McpSchema.CreateMessageResult.StopReason.END_TURN));
			}), client -> {
				StepVerifier.create(client.initialize()).expectNextMatches(Objects::nonNull).verifyComplete();

				StepVerifier.create(client.callTool(
						new McpSchema.CallToolRequest("sampleLLM", Map.of("prompt", message, "maxTokens", maxTokens))))
					.consumeNextWith(result -> {
						// Verify tool response to ensure our sampling response was passed
						// through
						assertThat(result.content()).hasAtLeastOneElementOfType(McpSchema.TextContent.class);
						assertThat(result.content()).allSatisfy(content -> {
							if (!(content instanceof McpSchema.TextContent text))
								return;

							assertThat(text.text()).endsWith(response); // Prefixed
						});

						// Verify sampling request parameters received in our callback
						assertThat(receivedPrompt.get()).isNotEmpty();
						assertThat(receivedMessage.get()).endsWith(message); // Prefixed
						assertThat(receivedMaxTokens.get()).isEqualTo(maxTokens);
					})
					.verifyComplete();
			});
	}

	@Test
	void testPing2() {
		withClient(createMcpTransport(), mcpAsyncClient -> {
			StepVerifier.create(mcpAsyncClient.initialize().then(mcpAsyncClient.ping()))
				.expectNextCount(1)
				.verifyComplete();
		});
	}

	protected Duration getRequestTimeout() {
		return Duration.ofSeconds(1400);
	}

	protected Duration getInitializationTimeout() {
		return Duration.ofSeconds(2000);
	}

}
