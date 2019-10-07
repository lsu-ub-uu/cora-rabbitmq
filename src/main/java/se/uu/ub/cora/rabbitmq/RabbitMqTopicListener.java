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
import java.util.Map;
import java.util.concurrent.TimeoutException;

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
		tryTolisten(messageReceiver);
	}

	private void tryTolisten(MessageReceiver messageReceiver) {
		try {
			listenMessages(messageReceiver);

		} catch (Exception e) {
			throw new MessagingInitializationException(e.getMessage());
		}
	}

	private void listenMessages(MessageReceiver messageReceiver)
			throws IOException, TimeoutException {
		connectionFactory.setHost(messagingRoutingInfo.hostname);
		connectionFactory.setPort(Integer.parseInt(messagingRoutingInfo.port));
		connectionFactory.setVirtualHost(messagingRoutingInfo.virtualHost);

		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		String queueName = channel.queueDeclare().getQueue();

		channel.queueBind(queueName, messagingRoutingInfo.exchange,
				messagingRoutingInfo.routingKey);

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			Map<String, Object> headers = delivery.getProperties().getHeaders();
			String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
			messageReceiver.receiveMessage(headers, message);
		};
		CancelCallback cancelCallback = consumerTag -> {
		};

		channel.basicConsume(queueName, true, deliverCallback, cancelCallback);
	}

}
