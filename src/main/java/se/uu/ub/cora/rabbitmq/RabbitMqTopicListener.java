/*
 * Copyright 2019, 2023, 2025 Uppsala University Library
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

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
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
			throwCorrectErrorMessage(e);
		}
	}

	private void tryTolisten(MessageReceiver messageReceiver) throws IOException, TimeoutException {
		setupConnectionFactory();
		connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();

		if (autoCreateQueue()) {
			createNewAutoCreadedChannel(channel);
		}
		startListening(messageReceiver, channel);
	}

	private void createNewAutoCreadedChannel(Channel channel) throws IOException {
		DeclareOk queueDeclare = channel.queueDeclare();
		String queueName = queueDeclare.getQueue();
		channel.queueBind(queueName, routingInfo.exchange, routingInfo.routingKey);
		routingInfo.queueName = queueName;
	}

	private boolean autoCreateQueue() {
		return routingInfo.exchange != null;
	}

	private void setupConnectionFactory() {
		connectionFactory.setHost(routingInfo.hostname);
		connectionFactory.setPort(routingInfo.port);
		connectionFactory.setVirtualHost(routingInfo.virtualHost);
	}

	private void startListening(MessageReceiver messageReceiver, Channel channel)
			throws IOException {
		boolean autoAck = false;
		DeliverCallback deliverCallback = getDeliverCallback(messageReceiver, channel);
		CancelCallback cancelCallback = getCancelCallback(messageReceiver, channel);
		channel.basicConsume(routingInfo.queueName, autoAck, deliverCallback, cancelCallback);
	}

	private DeliverCallback getDeliverCallback(MessageReceiver messageReceiver, Channel channel) {
		return (_, delivery) -> {
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
		return _ -> {
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

	private void throwCorrectErrorMessage(Exception e) {
		if (autoCreateQueue()) {
			throwErrorForAutoCreateQueue(e);
		}
		throwErrorForKnownQueue(e);
	}

	private void throwErrorForAutoCreateQueue(Exception e) {
		String errorMessage = "Error trying to listen rabbitMQ queue on "
				+ "{0}:{1}, vhost: {2}, exchange: {3} and routingKey: {4}";

		throw new MessagingInitializationException(MessageFormat.format(errorMessage,
				routingInfo.hostname, String.valueOf(routingInfo.port), routingInfo.virtualHost,
				routingInfo.exchange, routingInfo.routingKey), e);
	}

	private void throwErrorForKnownQueue(Exception e) {
		String errorMessage = "Error trying to listen rabbitMQ queue on "
				+ "{0}:{1}, vhost: {2} and queue: {3}";

		throw new MessagingInitializationException(MessageFormat.format(errorMessage,
				routingInfo.hostname, String.valueOf(routingInfo.port), routingInfo.virtualHost,
				routingInfo.queueName), e);
	}

	ConnectionFactory onlyForTestGetConnectionFactory() {
		return connectionFactory;
	}

	MessageRoutingInfo onlyForTestGetMessageRoutingInfo() {
		return routingInfo;
	}
}
