/*
 * Copyright 2019 Uppsala University Library
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import se.uu.ub.cora.messaging.AmqpMessageRoutingInfo;
import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageReceiver;
import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessagingInitializationException;

public class RabbitMqTopicListener implements MessageListener {

	private ConnectionFactory connectionFactory;
	private AmqpMessageRoutingInfo messagingRoutingInfo;
	private Connection connection;
	private Channel channel;

	public static RabbitMqTopicListener usingConnectionFactoryAndMessageRoutingInfo(
			ConnectionFactory connectionFactory, MessageRoutingInfo messagingRoutingInfo) {
		return new RabbitMqTopicListener(connectionFactory,
				(AmqpMessageRoutingInfo) messagingRoutingInfo);
	}

	private RabbitMqTopicListener(ConnectionFactory connectionFactory,
			AmqpMessageRoutingInfo messagingRoutingInfo) {
		this.connectionFactory = connectionFactory;
		this.messagingRoutingInfo = messagingRoutingInfo;
	}

	@Override
	public void listen(MessageReceiver messageReceiver) {
		try {
			tryTolisten(messageReceiver);
		} catch (Exception e) {
			throw new MessagingInitializationException(e.getMessage(), e);
		}
	}

	private void tryTolisten(MessageReceiver messageReceiver) throws IOException, TimeoutException {
		setupConnectionFactory();
		connection = connectionFactory.newConnection();
		channel = connection.createChannel();
		listenMessages(messageReceiver, channel);
	}

	private void listenMessages(MessageReceiver messageReceiver, Channel channel)
			throws IOException {
		startListening(messageReceiver, channel);
	}

	private void startListening(MessageReceiver messageReceiver, Channel channel)
			throws IOException {
		String queueName = bindQueue(channel);
		boolean autoAck = true;
		DeliverCallback deliverCallback = getDeliverCallback(messageReceiver);
		CancelCallback cancelCallback = getCancelCallback(messageReceiver);

		channel.basicConsume(queueName, autoAck, deliverCallback, cancelCallback);
	}

	private void setupConnectionFactory() {
		connectionFactory.setHost(messagingRoutingInfo.hostname);
		connectionFactory.setPort(Integer.parseInt(messagingRoutingInfo.port));
		connectionFactory.setVirtualHost(messagingRoutingInfo.virtualHost);
	}

	private String bindQueue(Channel channel) throws IOException {
		String queueName = channel.queueDeclare().getQueue();
		channel.queueBind(queueName, messagingRoutingInfo.exchange,
				messagingRoutingInfo.routingKey);
		return queueName;
	}

	private DeliverCallback getDeliverCallback(MessageReceiver messageReceiver) {
		return (consumerTag, delivery) -> {

			Map<String, Object> nativeHeaders = delivery.getProperties().getHeaders();
			Map<String, String> headers = new HashMap<>();
			nativeHeaders.forEach((key, value) -> extracted(headers, key, value));

			String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
			messageReceiver.receiveMessage(headers, message);
		};
	}

	private String extracted(Map<String, String> headers, String key, Object value) {
		return headers.put(key, String.valueOf(value));
	}

	private CancelCallback getCancelCallback(MessageReceiver messageReceiver) {
		return consumerTag -> {
			messageReceiver.topicClosed();
			tryToCloseConnection();
		};
	}

	private void tryToCloseConnection() {
		try {
			channel.close();
			connection.close();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	ConnectionFactory getConnectionFactory() {
		// needed for test
		return connectionFactory;
	}

	MessageRoutingInfo getMessageRoutingInfo() {
		// needed for test
		return messagingRoutingInfo;
	}
}
