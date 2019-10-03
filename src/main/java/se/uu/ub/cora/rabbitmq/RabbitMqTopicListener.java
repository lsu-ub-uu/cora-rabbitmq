package se.uu.ub.cora.rabbitmq;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.ConnectionFactory;

import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageReceiver;
import se.uu.ub.cora.messaging.MessageRoutingInfo;

public class RabbitMqTopicListener implements MessageListener {

	private ConnectionFactory connectionFactory;
	private MessageRoutingInfo messagingRoutingInfo;

	public static RabbitMqTopicListener usingConnectionFactoryAndMessageRoutingInfo(
			ConnectionFactory connectionFactory, MessageRoutingInfo messagingRoutingInfo) {
		return new RabbitMqTopicListener(connectionFactory, messagingRoutingInfo);
	}

	private RabbitMqTopicListener(ConnectionFactory connectionFactory,
			MessageRoutingInfo messagingRoutingInfo) {
		this.connectionFactory = connectionFactory;
		this.messagingRoutingInfo = messagingRoutingInfo;
	}

	ConnectionFactory getConnectionFactory() {
		// needed for test
		return connectionFactory;
	}

	MessageRoutingInfo getMessageRoutingInfo() {
		// needed for test
		return messagingRoutingInfo;
	}

	@Override
	public void listen(MessageReceiver messageReceiver) {

		try {
			connectionFactory.setHost(messagingRoutingInfo.hostname);
			connectionFactory.setPort(Integer.valueOf(messagingRoutingInfo.port).intValue());
			connectionFactory.setVirtualHost(messagingRoutingInfo.virtualHost);

			connectionFactory.newConnection();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
