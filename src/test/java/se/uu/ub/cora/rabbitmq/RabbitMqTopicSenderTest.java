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

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

import se.uu.ub.cora.messaging.AmqpMessageRoutingInfo;
import se.uu.ub.cora.messaging.MessageSender;
import se.uu.ub.cora.messaging.MessagingInitializationException;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqChannelSpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionFactorySpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionSpy;

public class RabbitMqTopicSenderTest {

	private RabbitMqConnectionFactorySpy rabbitFactorySpy;
	private AmqpMessageRoutingInfo routingInfo;
	private RabbitMqTopicSender messageSender;

	@BeforeMethod
	public void beforeMethod() {
		rabbitFactorySpy = new RabbitMqConnectionFactorySpy();
		routingInfo = new AmqpMessageRoutingInfo("messaging.alvin-portal.org", "5672",
				"alvin", "index", "alvin.updates.#");
		messageSender = RabbitMqTopicSender
				.usingConnectionFactoryAndMessageRoutingInfo(rabbitFactorySpy, routingInfo);
	}

	@Test
	public void testImplementsMessageSender() throws Exception {
		assertTrue(messageSender instanceof MessageSender);
	}

	@Test
	public void testSetConnectionFactoryChannel() throws Exception {
		int portAsInt = Integer.valueOf(routingInfo.port).intValue();

		assertEquals(rabbitFactorySpy.host, routingInfo.hostname);
		assertEquals(rabbitFactorySpy.port, portAsInt);
		assertEquals(rabbitFactorySpy.virtualHost, routingInfo.virtualHost);
	}

	@Test
	public void testSendMessageCreatesAConnection() throws Exception {
		assertEquals(rabbitFactorySpy.createdConnections.size(), 0);
		messageSender.sendMessage(null, "");
		assertEquals(rabbitFactorySpy.createdConnections.size(), 1);
	}

	@Test(expectedExceptions = MessagingInitializationException.class, expectedExceptionsMessageRegExp = ""
			+ "Error from RabbitMqConnectionFactorySpy on newConnection")
	public void testExceptionHandlingOnSendMessage() throws Exception {
		rabbitFactorySpy.throwErrorOnSendMessage = true;
		messageSender.sendMessage(null, "");
	}

	@Test
	public void testSendMessageCreatesChannel() throws Exception {
		messageSender.sendMessage(null, "");
		RabbitMqConnectionSpy firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		assertEquals(firstCreatedConnection.createdChannels.size(), 1);
	}

	@Test
	public void testPublishEmptyMessage() throws Exception {
		String message = "";
		Map<String, Object> headers = new HashMap<>();
		messageSender.sendMessage(headers, message);
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
