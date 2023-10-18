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

import se.uu.ub.cora.messaging.AmqpMessageListenerRoutingInfo;
import se.uu.ub.cora.messaging.AmqpMessageSenderRoutingInfo;
import se.uu.ub.cora.messaging.MessagingFactory;

public class RabbitMqMessagingFactoryTest {

	private static final String SOME_HOST = "someHostname";
	private static final int SOME_PORT = 8080;
	private static final String SOME_VHOST = "someVirtualHost";
	private static final String SOME_QUEUE = "someQueue";
	private static final String SOME_EXCHANGE = "someExchange";
	private static final String SOME_ROUTING_KEY = "someRoutingKey";

	AmqpMessageListenerRoutingInfo routingInfo;

	@BeforeTest
	public void beforeTest() {
		// routingInfo = new AmqpMessageListenerRoutingInfo("messaging.alvin-portal.org", "5672",
		// "alvin", "index", "alvin.updates.#");
		new AmqpMessageSenderRoutingInfo(SOME_HOST, SOME_PORT, SOME_VHOST, SOME_EXCHANGE,
				SOME_ROUTING_KEY);
		routingInfo = new AmqpMessageListenerRoutingInfo(SOME_HOST, SOME_PORT, SOME_VHOST,
				SOME_QUEUE);
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
		assertTrue(messageSender.onlyForTestGetConnectionFactory() instanceof ConnectionFactory);
		assertEquals(messageSender.onlyForTestGetMessageRoutingInfo(), routingInfo);
	}

	@Test
	public void testFactorReturnsRabbitMqTopicListener() throws Exception {
		RabbitMqMessagingFactory factory = new RabbitMqMessagingFactory();

		RabbitMqTopicListener messageListener = (RabbitMqTopicListener) factory
				.factorTopicMessageListener(routingInfo);

		assertTrue(messageListener instanceof RabbitMqTopicListener);
		assertTrue(messageListener.onlyForTestGetConnectionFactory() instanceof ConnectionFactory);
		assertEquals(messageListener.onlyForTestGetMessageRoutingInfo(), routingInfo);
	}
}
