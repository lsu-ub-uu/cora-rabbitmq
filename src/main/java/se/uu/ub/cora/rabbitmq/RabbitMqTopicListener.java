package se.uu.ub.cora.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageReceiver;
import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessagingInitializationException;

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
			connectionFactory.setPort(Integer.parseInt(messagingRoutingInfo.port));
			connectionFactory.setVirtualHost(messagingRoutingInfo.virtualHost);

			Connection connection = connectionFactory.newConnection();
			Channel channel = connection.createChannel();
			String queueName = channel.queueDeclare().getQueue();

			channel.queueBind(queueName, messagingRoutingInfo.exchange,
					messagingRoutingInfo.routingKey);

			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				Map<String, Object> headers = null;
				String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
				// String message = new String(delivery.getBody());
				messageReceiver.receiveMessage(headers, message);
			};
			CancelCallback cancelCallback = consumerTag -> {
			};

			channel.basicConsume(queueName, true, deliverCallback, cancelCallback);

		} catch (Exception e) {
			throw new MessagingInitializationException(e.getMessage());
		}
	}

}
