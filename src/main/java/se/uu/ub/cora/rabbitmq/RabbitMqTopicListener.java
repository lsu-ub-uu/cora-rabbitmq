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

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;

import se.uu.ub.cora.messaging.AmqpMessageRoutingInfo;
import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageReceiver;
import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessagingInitializationException;

public class RabbitMqTopicListener implements MessageListener {

	private ConnectionFactory connectionFactory;
	private AmqpMessageRoutingInfo routingInfo;
	private Connection connection;

	public static RabbitMqTopicListener usingConnectionFactoryAndMessageRoutingInfo(
			ConnectionFactory connectionFactory, MessageRoutingInfo routingInfo) {
		return new RabbitMqTopicListener(connectionFactory, (AmqpMessageRoutingInfo) routingInfo);
	}

	private RabbitMqTopicListener(ConnectionFactory connectionFactory,
			AmqpMessageRoutingInfo routingInfo) {
		this.connectionFactory = connectionFactory;
		this.routingInfo = routingInfo;
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
		Channel channel = connection.createChannel();

		///

		channel.exchangeDeclare(routingInfo.exchange, BuiltinExchangeType.DIRECT, true);
		channel.queueDeclare("workerQ", true, false, false, null);
		channel.queueBind("workerQ", routingInfo.exchange,
				routingInfo.routingKey);
		channel.basicQos(1);
		///

		listenMessages(messageReceiver, channel);
	}

	private void listenMessages(MessageReceiver messageReceiver, Channel channel)
			throws IOException {
		startListening(messageReceiver, channel);
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

		channel.basicConsume("workerQ", autoAck, deliverCallback, cancelCallback);

		// channel.basicConsume("TASK_QUEUE_NAME", autoAck, deliverCallback, cancelCallback);
		// try {
		// Thread.sleep(10000);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	private void setupConnectionFactory() {
		connectionFactory.setHost(routingInfo.hostname);
		connectionFactory.setPort(Integer.parseInt(routingInfo.port));
		connectionFactory.setVirtualHost(routingInfo.virtualHost);
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
			// System.out.println("here1");
			Map<String, Object> nativeHeaders = delivery.getProperties().getHeaders();
			// System.out.println("here12");
			Map<String, String> headers = new HashMap<>();
			// System.out.println("here13");
			// System.out.println("here13: " + nativeHeaders);

			nativeHeaders.forEach((key, value) -> extracted(headers, key, value));
			// System.out.println("here2");

			String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
			// System.out.println("here3");

			// long deliveryTag = delivery.getEnvelope().getDeliveryTag();
			// channel.basicAck(deliveryTag, false);
			// channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

			// messageReceiver, need possibility to do channel.basicAck
			messageReceiver.receiveMessage(headers, message);
			System.out.println("doing ack after work...");
			try {

				Envelope envelope = delivery.getEnvelope();
				long deliveryTag = envelope.getDeliveryTag();
				System.out.println("deliveryTag: " + deliveryTag);
				channel.basicAck(deliveryTag, false);
			} catch (Exception e) {
				System.err.println(e);
			}
			System.out.println("doing ack after work...later");
		};
	}

	private String extracted(Map<String, String> headers, String key, Object value) {
		return headers.put(key, String.valueOf(value));
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
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	ConnectionFactory getConnectionFactory() {
		// needed for test
		return connectionFactory;
	}

	MessageRoutingInfo getMessageRoutingInfo() {
		// needed for test
		return routingInfo;
	}
}
