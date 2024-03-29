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
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import se.uu.ub.cora.messaging.AmqpMessageSenderRoutingInfo;
import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessageSender;
import se.uu.ub.cora.messaging.MessagingInitializationException;

/**
 * RabbitMqTopicSender is an implementation of {@link MessageSender} for RabbitMQ
 *
 */

public class RabbitMqTopicSender implements MessageSender {

	public static RabbitMqTopicSender usingConnectionFactoryAndMessageRoutingInfoSender(
			ConnectionFactory rabbitFactory, AmqpMessageSenderRoutingInfo routingInfo) {
		return new RabbitMqTopicSender(rabbitFactory, routingInfo);
	}

	private ConnectionFactory rabbitFactory;
	private AmqpMessageSenderRoutingInfo routingInfo;

	private RabbitMqTopicSender(ConnectionFactory rabbitFactory,
			AmqpMessageSenderRoutingInfo routingInfo) {

		this.rabbitFactory = rabbitFactory;
		this.routingInfo = routingInfo;
		rabbitFactory.setHost(routingInfo.hostname);
		rabbitFactory.setPort(routingInfo.port);
		rabbitFactory.setVirtualHost(routingInfo.virtualHost);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @param headers
	 *            {@inheritDoc}. How objects are serialized is up to the implementation in RabbitMQ
	 */
	@Override
	public void sendMessage(Map<String, Object> headers, String message) {
		tryToSendMessage(headers, message);
	}

	private void tryToSendMessage(Map<String, Object> headers, String message) {
		try (Connection connection = rabbitFactory.newConnection();
				Channel channel = connection.createChannel()) {

			/// MOVE TO rabbit SERVER configuration
			// channel.exchangeDeclare(routingInfo.exchange, BuiltinExchangeType.DIRECT, true);
			// channel.queueDeclare("workerQ", true, false, false, null);
			// channel.queueBind("workerQ", routingInfo.exchange, routingInfo.routingKey);
			// channel.basicQos(1);
			///

			publishMessage(headers, message, channel);
		} catch (Exception e) {
			throw new MessagingInitializationException(e.getMessage(), e);
		}
	}

	private void publishMessage(Map<String, Object> headers, String message, Channel channel)
			throws IOException {

		String exchange = routingInfo.exchange;
		String routingKey = routingInfo.routingKey;
		BasicProperties props = createPropertiesWithHeaders(headers);
		byte[] bytes = message.getBytes(StandardCharsets.UTF_8);

		channel.basicPublish(exchange, routingKey, props, bytes);
	}

	private BasicProperties createPropertiesWithHeaders(Map<String, Object> headers) {
		AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
		builder.headers(headers);
		return builder.build();
	}

	ConnectionFactory onlyForTestGetConnectionFactory() {
		return rabbitFactory;
	}

	MessageRoutingInfo onlyForTestGetMessageRoutingInfo() {
		return routingInfo;
	}

}
