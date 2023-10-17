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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

import se.uu.ub.cora.messaging.AmqpMessageRoutingInfoSender;
import se.uu.ub.cora.messaging.MessageSender;
import se.uu.ub.cora.messaging.MessagingInitializationException;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqChannelSpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionFactorySpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionSpy;

public class RabbitMqTopicSenderTest {

	private static final String SOME_HOST = "someHostname";
	private static final String SOME_PORT = "8080";
	private static final String SOME_VHOST = "someVirtualHost";
	private static final String SOME_EXCHANGE = "someExchange";
	private static final String SOME_ROUTING_KEY = "someRoutingKey";

	private RabbitMqConnectionFactorySpy rabbitFactorySpy;
	private AmqpMessageRoutingInfoSender routingInfo;
	private RabbitMqTopicSender messageSender;

	@BeforeMethod
	public void beforeMethod() {
		rabbitFactorySpy = new RabbitMqConnectionFactorySpy();
		routingInfo = new AmqpMessageRoutingInfoSender(SOME_HOST, SOME_PORT, SOME_VHOST,
				SOME_EXCHANGE, SOME_ROUTING_KEY);

		messageSender = RabbitMqTopicSender
				.usingConnectionFactoryAndMessageRoutingInfoSender(rabbitFactorySpy, routingInfo);
	}

	@Test
	public void testImplementsMessageSender() throws Exception {
		assertTrue(messageSender instanceof MessageSender);
	}

	@Test
	public void testSetConnectionFactoryChannel() throws Exception {
		rabbitFactorySpy.MCR.assertParameters("setHost", 0, SOME_HOST);
		rabbitFactorySpy.MCR.assertParameters("setPort", 0, Integer.valueOf(SOME_PORT));
		rabbitFactorySpy.MCR.assertParameters("setVirtualHost", 0, SOME_VHOST);
	}

	@Test
	public void testSendMessageCreatesAConnection() throws Exception {
		rabbitFactorySpy.MCR.assertMethodNotCalled("newConnection");
		messageSender.sendMessage(null, "");
		rabbitFactorySpy.MCR.assertMethodWasCalled("newConnection");
	}

	@Test(expectedExceptions = MessagingInitializationException.class, expectedExceptionsMessageRegExp = ""
			+ "Error from RabbitMqConnectionFactorySpy on newConnection")
	public void testExceptionHandlingOnSendMessage() throws Exception {
		rabbitFactorySpy.MRV.setAlwaysThrowException("newConnection",
				new RuntimeException("Error from RabbitMqConnectionFactorySpy on newConnection"));
		messageSender.sendMessage(null, "");
	}

	@Test
	public void testExceptionHandlingOnSendMessageSendsAlongInitialException() throws Exception {
		rabbitFactorySpy.MRV.setAlwaysThrowException("newConnection",
				new RuntimeException("Error from RabbitMqConnectionFactorySpy on newConnection"));
		try {
			messageSender.sendMessage(null, "");
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof RuntimeException);
		}
	}

	@Test
	public void testSendMessageCreatesChannel() throws Exception {
		messageSender.sendMessage(null, "");
		RabbitMqConnectionSpy connection = getConnection();
		connection.MCR.assertParameters("createChannel", 0);
		connection.MCR.assertNumberOfCallsToMethod("createChannel", 1);
	}

	private RabbitMqConnectionSpy getConnection() {
		RabbitMqConnectionSpy connection = (RabbitMqConnectionSpy) rabbitFactorySpy.MCR
				.getReturnValue("newConnection", 0);
		return connection;
	}

	@Test
	public void testPublishEmptyMessage() throws Exception {

		messageSender.sendMessage(Collections.emptyMap(), "");

		RabbitMqConnectionSpy connection = getConnection();

		RabbitMqConnectionSpy firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		RabbitMqChannelSpy firstCreatedChannel = firstCreatedConnection.createdChannels.get(0);
		assertEquals(firstCreatedChannel.publishedMessages.size(), 1);
	}

	@Test
	public void testPublishMessage() throws Exception {
		String message = "{\"pid\":\"alvin-place:1\",\"routingKey\":\"alvin.updates.place\","
				+ "\"action\":\"UPDATE\",\"dsId\":null,"
				+ "\"headers\":{\"ACTION\":\"UPDATE\",\"PID\":\"alvin-place:1\"}}";
		messageSender.sendMessage(null, message);
		Map<String, Object> publishedMessage = getFirstPublishedMessage();

		assertEquals(publishedMessage.get("body"), message.getBytes());
	}

	private Map<String, Object> getFirstPublishedMessage() {
		RabbitMqConnectionSpy firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		RabbitMqChannelSpy firstCreatedChannel = firstCreatedConnection.createdChannels.get(0);
		Map<String, Object> publishedMessage = firstCreatedChannel.publishedMessages.get(0);
		return publishedMessage;
	}

	@Test
	public void testPublishMessageHeader() throws Exception {
		Map<String, Object> headers = new HashMap<>();
		headers.put("__TypeId__", "epc.messaging.amqp.EPCFedoraMessage");
		headers.put("ACTION", "UPDATE");
		headers.put("PID", "alvin-place:1");
		headers.put("messageSentFrom", "Cora");
		messageSender.sendMessage(headers, "");

		Map<String, Object> publishedMessage = getFirstPublishedMessage();

		BasicProperties publishedProps = (BasicProperties) publishedMessage.get("props");
		assertTrue(publishedProps instanceof AMQP.BasicProperties);
		Map<String, Object> publishedHeaders = publishedProps.getHeaders();
		assertEquals(publishedProps.getContentType(), "application/json");
		assertEquals(publishedHeaders.get("__TypeId__"), headers.get("__TypeId__"));
		assertEquals(publishedHeaders.get("ACTION"), headers.get("ACTION"));
		assertEquals(publishedHeaders.get("PID"), headers.get("PID"));
		assertEquals(publishedHeaders.get("messageSentFrom"), headers.get("messageSentFrom"));
	}

	@Test
	public void testPublishTopic() throws Exception {
		messageSender.sendMessage(null, "");
		Map<String, Object> publishedMessage = getFirstPublishedMessage();
		assertEquals(publishedMessage.get("routingKey"), routingInfo.routingKey);
	}

	@Test
	public void testPublishExchangeName() throws Exception {
		messageSender.sendMessage(null, "");
		Map<String, Object> publishedMessage = getFirstPublishedMessage();
		assertEquals(publishedMessage.get("exchange"), routingInfo.exchange);
	}

	@Test
	public void testCloseConnection() throws Exception {
		messageSender.sendMessage(null, "");
		RabbitMqConnectionSpy firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		RabbitMqChannelSpy firstCreatedChannel = firstCreatedConnection.createdChannels.get(0);
		assertEquals(firstCreatedChannel.closeHasBeenCalled, true);
		assertEquals(firstCreatedConnection.closeHasBeenCalled, true);
	}
}
