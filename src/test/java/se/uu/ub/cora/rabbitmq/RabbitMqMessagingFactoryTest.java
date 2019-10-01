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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.rabbitmq.client.ConnectionFactory;

import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessagingFactory;

public class RabbitMqMessagingFactoryTest {

	@Test
	public void testInit() throws Exception {
		RabbitMqMessagingFactory factory = new RabbitMqMessagingFactory();

		assertTrue(factory instanceof MessagingFactory);
	}

	@Test
	public void testFactorReturnsRabbitMqTopicSender() throws Exception {
		String hostname = "messaging.alvin-portal.org";
		String port = "5672";
		String virtualHost = "alvin";
		String exchange = "index";
		String routingKey = "alvin.updates.#";
		RabbitMqMessagingFactory factory = new RabbitMqMessagingFactory();
		MessageRoutingInfo messageRoutingInfo = new MessageRoutingInfo(hostname, port, virtualHost,
				exchange, routingKey);

		RabbitMqTopicSender messageSender = (RabbitMqTopicSender) factory
				.factorTopicMessageSender(messageRoutingInfo);

		assertTrue(messageSender instanceof RabbitMqTopicSender);
		assertTrue(messageSender.getConnectionFactory() instanceof ConnectionFactory);
		assertEquals(messageSender.getMessageRoutingInfo(), messageRoutingInfo);

	}
}
