package se.uu.ub.cora.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqConnectionFactorySpy extends ConnectionFactory {

	public String host;
	public int port;
	public String virtualHost;
	public List<Connection> createdConnections = new ArrayList<>();
	public boolean throwErrorOnSendMessage = false;

	@Override
	public void setHost(String host) {
		this.host = host;
	}

	@Override
	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void setVirtualHost(String virtualHost) {
		this.virtualHost = virtualHost;
	}

	@Override
	public Connection newConnection() throws IOException, TimeoutException {
		if (throwErrorOnSendMessage) {
			throw new RuntimeException("Error from RabbitMqConnectionFactorySpy on newConnection");
		}
		RabbitMqConnectionSpy rabbitMqConnectionSpy = new RabbitMqConnectionSpy();
		createdConnections.add(rabbitMqConnectionSpy);
		return rabbitMqConnectionSpy;
	}
}
