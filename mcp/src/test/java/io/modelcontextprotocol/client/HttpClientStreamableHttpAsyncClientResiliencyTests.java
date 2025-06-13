package io.modelcontextprotocol.client;

import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.SimpleWebClient;
import io.modelcontextprotocol.spec.McpClientTransport;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
public class HttpClientStreamableHttpAsyncClientResiliencyTests extends AbstractMcpAsyncClientResiliencyTests {

	@Override
	protected McpClientTransport createMcpTransport() {
		return HttpClientStreamableHttpTransport.builder(SimpleWebClient.builder().baseUrl(host)).build();
	}

}
