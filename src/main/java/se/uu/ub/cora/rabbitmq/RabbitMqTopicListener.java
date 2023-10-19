/*
 * Copyright 2019, 2023 Uppsala University Library
 *
 * This file is part of Cora.
 *
 *     Cora is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Cora is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Cora.  If not, see <http://www.gnu.org/licenses/>.
 */
package se.uu.ub.cora.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import se.uu.ub.cora.messaging.AmqpMessageListenerRoutingInfo;
import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageReceiver;
import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessagingInitializationException;

public class RabbitMqTopicListener implements MessageListener {

	private ConnectionFactory connectionFactory;
	private AmqpMessageListenerRoutingInfo routingInfo;
	private Connection connection;

	public static RabbitMqTopicListener usingConnectionFactoryAndMessageRoutingInfo(
			ConnectionFactory connectionFactory, AmqpMessageListenerRoutingInfo routingInfo) {
		return new RabbitMqTopicListener(connectionFactory, routingInfo);
	}

	private RabbitMqTopicListener(ConnectionFactory connectionFactory,
			AmqpMessageListenerRoutingInfo routingInfo) {
		this.connectionFactory = connectionFactory;
		this.routingInfo = routingInfo;
	}

	@Override
	public void listen(MessageReceiver messageReceiver) {
		try {
			tryTolisten(messageReceiver);
		} catch (Exception e) {
			String errorMessage = "Error trying to listen rabbitMQ queue on "
					+ "{0}:{1}, vhost: {2} and queue: {3}";

			throw new MessagingInitializationException(MessageFormat.format(errorMessage,
					routingInfo.hostname, String.valueOf(routingInfo.port), routingInfo.virtualHost,
					routingInfo.queueName), e);
		}
	}

	private void tryTolisten(MessageReceiver messageReceiver) throws IOException, TimeoutException {
		setupConnectionFactory();
		connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();

		///
		/// MOVE TO rabbit SERVER configuration
		// channel.exchangeDeclare(routingInfo.exchange, BuiltinExchangeType.DIRECT, true);
		// channel.queueDeclare("workerQ", true, false, false, null);
		// channel.queueBind("workerQ", routingInfo.exchange, routingInfo.routingKey);
		// channel.basicQos(1);
		///

		startListening(messageReceiver, channel);
	}

	private void setupConnectionFactory() {
		connectionFactory.setHost(routingInfo.hostname);
		connectionFactory.setPort(routingInfo.port);
		connectionFactory.setVirtualHost(routingInfo.virtualHost);
	}

	private void startListening(MessageReceiver messageReceiver, Channel channel)
			throws IOException {
		// String queueName = bindQueue(channel);
		// System.out.println(queueName);
		// channel.queueDeclare("TASK_QUEUE_NAME", true, false, false, null);

		// needs to be possible to change
		boolean autoAck = false;
		DeliverCallback deliverCallback = getDeliverCallback(messageReceiver, channel);
		CancelCallback cancelCallback = getCancelCallback(messageReceiver, channel);

		channel.basicConsume(routingInfo.queueName, autoAck, deliverCallback, cancelCallback);

		// channel.basicConsume("TASK_QUEUE_NAME", autoAck, deliverCallback, cancelCallback);
		// try {
		// Thread.sleep(10000);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	// private String bindQueue(Channel channel) throws IOException {
	// String queueName = channel.queueDeclare().getQueue();
	// // channel.basicQos(1);
	// // channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
	// channel.queueBind(queueName, messagingRoutingInfo.exchange,
	// messagingRoutingInfo.routingKey);
	// return queueName;
	// }

	private DeliverCallback getDeliverCallback(MessageReceiver messageReceiver, Channel channel) {
		return (consumerTag, delivery) -> {
			Map<String, String> headers = getHeadersFromDelivery(delivery);
			String message = getMessageFromDelivery(delivery);
			messageReceiver.receiveMessage(headers, message);

			acknowledgeMessage(channel, delivery);
		};
	}

	private Map<String, String> getHeadersFromDelivery(Delivery delivery) {
		Map<String, Object> nativeHeaders = delivery.getProperties().getHeaders();
		Map<String, String> headers = new HashMap<>();

		nativeHeaders.forEach((key, value) -> putHeaderInMap(headers, key, value));
		return headers;
	}

	private String putHeaderInMap(Map<String, String> headers, String key, Object value) {
		return headers.put(key, String.valueOf(value));
	}

	private String getMessageFromDelivery(Delivery delivery) {
		return new String(delivery.getBody(), StandardCharsets.UTF_8);
	}

	private void acknowledgeMessage(Channel channel, Delivery delivery) throws IOException {
		Envelope envelope = delivery.getEnvelope();
		long deliveryTag = envelope.getDeliveryTag();
		channel.basicAck(deliveryTag, false);
	}

	private CancelCallback getCancelCallback(MessageReceiver messageReceiver, Channel channel) {
		return consumerTag -> {
			messageReceiver.topicClosed();
			tryToCloseConnection(channel);
		};
	}

	private void tryToCloseConnection(Channel channel) {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			throw new RuntimeException(
					"Error closing channel and connection in RabbitMqTopicListener", e);
		}
	}

	ConnectionFactory onlyForTestGetConnectionFactory() {
		return connectionFactory;
	}

	MessageRoutingInfo onlyForTestGetMessageRoutingInfo() {
		return routingInfo;
	}
}
