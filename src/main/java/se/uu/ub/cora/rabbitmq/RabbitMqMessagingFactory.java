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

import com.rabbitmq.client.ConnectionFactory;

import se.uu.ub.cora.messaging.AmqpMessageListenerRoutingInfo;
import se.uu.ub.cora.messaging.AmqpMessageSenderRoutingInfo;
import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessageSender;
import se.uu.ub.cora.messaging.MessagingFactory;

/**
 * Implementation of {@link MessagingFactory} for RabbitMQ.
 */

public class RabbitMqMessagingFactory implements MessagingFactory {

	@Override
	public MessageSender factorTopicMessageSender(MessageRoutingInfo routingInfo) {
		return RabbitMqTopicSender.usingConnectionFactoryAndMessageRoutingInfoSender(
				new ConnectionFactory(), (AmqpMessageSenderRoutingInfo) routingInfo);
	}

	@Override
	public MessageListener factorTopicMessageListener(MessageRoutingInfo routingInfo) {
		return RabbitMqTopicListener.usingConnectionFactoryAndMessageRoutingInfo(
				new ConnectionFactory(), (AmqpMessageListenerRoutingInfo) routingInfo);
	}
}
