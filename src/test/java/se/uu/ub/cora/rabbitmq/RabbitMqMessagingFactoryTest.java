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

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.rabbitmq.client.ConnectionFactory;

import se.uu.ub.cora.messaging.AmqpMessageRoutingInfo;
import se.uu.ub.cora.messaging.MessagingFactory;

public class RabbitMqMessagingFactoryTest {

	AmqpMessageRoutingInfo routingInfo;

	@BeforeTest
	public void beforeTest() {
		routingInfo = new AmqpMessageRoutingInfo("messaging.alvin-portal.org", "5672",
				"alvin.updates.#", "alvin", "index");
	}

	@Test
	public void testInit() throws Exception {
		RabbitMqMessagingFactory factory = new RabbitMqMessagingFactory();

		assertTrue(factory instanceof MessagingFactory);
	}

	@Test
	public void testFactorReturnsRabbitMqTopicSender() throws Exception {
		RabbitMqMessagingFactory factory = new RabbitMqMessagingFactory();

		RabbitMqTopicSender messageSender = (RabbitMqTopicSender) factory
				.factorTopicMessageSender(routingInfo);

		assertTrue(messageSender instanceof RabbitMqTopicSender);
		assertTrue(messageSender.getConnectionFactory() instanceof ConnectionFactory);
		assertEquals(messageSender.getMessageRoutingInfo(), routingInfo);
	}

	@Test
	public void testFactorReturnsRabbitMqTopicListener() throws Exception {
		RabbitMqMessagingFactory factory = new RabbitMqMessagingFactory();

		RabbitMqTopicListener messageListener = (RabbitMqTopicListener) factory
				.factorTopicMessageListener(routingInfo);

		assertTrue(messageListener instanceof RabbitMqTopicListener);
		assertTrue(messageListener.getConnectionFactory() instanceof ConnectionFactory);
		assertEquals(messageListener.getMessageRoutingInfo(), routingInfo);
	}
}
