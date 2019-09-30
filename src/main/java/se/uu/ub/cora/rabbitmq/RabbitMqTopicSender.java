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

import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import se.uu.ub.cora.messaging.ChannelInfo;
import se.uu.ub.cora.messaging.MessageSender;
import se.uu.ub.cora.messaging.MessagingInitializationException;

public class RabbitMqTopicSender implements MessageSender {

	private ConnectionFactory rabbitFactory;

	public RabbitMqTopicSender(ConnectionFactory rabbitFactory, ChannelInfo channelInfo) {
		this.rabbitFactory = rabbitFactory;
		rabbitFactory.setHost(channelInfo.hostname);
		rabbitFactory.setPort(Integer.valueOf(channelInfo.port));
		rabbitFactory.setVirtualHost(channelInfo.virtualHost);
	}

	@Override
	public void sendMessage(Map<String, Object> headers, String message) {
		try {
			Connection connection = rabbitFactory.newConnection();
			Channel channel = connection.createChannel();
			channel.basicPublish(null, null, null, message.getBytes());
		} catch (Exception e) {
			throw new MessagingInitializationException(e.getMessage());
		}
	}

}
