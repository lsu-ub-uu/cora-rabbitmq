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

import se.uu.ub.cora.messaging.ChannelInfo;
import se.uu.ub.cora.messaging.MessageSender;
import se.uu.ub.cora.messaging.MessagingInitializationException;

public class RabbitMqTopicSenderTest {

	private RabbitMqConnectionFactorySpy rabbitFactorySpy;
	private ChannelInfo channelInfo;
	private RabbitMqTopicSender messageSender;

	@BeforeMethod
	public void beforeMethod() {
		rabbitFactorySpy = new RabbitMqConnectionFactorySpy();
		channelInfo = new ChannelInfo("messaging.alvin-portal.org", "5672", "alvin",
				"alvin.updates.#");
		messageSender = new RabbitMqTopicSender(rabbitFactorySpy, channelInfo);
	}

	@Test
	public void testImplementsMessageSender() throws Exception {
		assertTrue(messageSender instanceof MessageSender);
	}

	@Test
	public void testSetConnectionFactoryChannel() throws Exception {
		int portAsInt = Integer.valueOf(channelInfo.port).intValue();

		assertEquals(rabbitFactorySpy.host, channelInfo.hostname);
		assertEquals(rabbitFactorySpy.port, portAsInt);
		assertEquals(rabbitFactorySpy.virtualHost, channelInfo.virtualHost);
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
		RabbitMqConnectionSpy firstCreatedConnection = (RabbitMqConnectionSpy) rabbitFactorySpy.createdConnections
				.get(0);
		assertEquals(firstCreatedConnection.createdChannels.size(), 1);
	}

	@Test
	public void testPublishEmptyMessage() throws Exception {
		String message = "";
		Map<String, Object> headers = new HashMap<>();
		messageSender.sendMessage(headers, message);
		RabbitMqConnectionSpy firstCreatedConnection = (RabbitMqConnectionSpy) rabbitFactorySpy.createdConnections
				.get(0);
		RabbitMqChannelSpy firstCreatedChannel = firstCreatedConnection.createdChannels.get(0);
		assertEquals(firstCreatedChannel.publishedMessages.size(), 1);
	}

	@Test
	public void testPublishMessageAndHeaders() throws Exception {
		String message = "{\"pid\":\"alvin-place:1\",\"routingKey\":\"alvin.updates.place\","
				+ "\"action\":\"UPDATE\",\"dsId\":null,"
				+ "\"headers\":{\"ACTION\":\"UPDATE\",\"PID\":\"alvin-place:1\"}}";
		Map<String, Object> headers = new HashMap<>();
		headers.put("__TypeId__", "epc.messaging.amqp.EPCFedoraMessage");
		headers.put("ACTION", "UPDATE");
		headers.put("PID", "alvin-place:1");
		headers.put("messageSentFrom", "Cora");
		messageSender.sendMessage(headers, message);
		RabbitMqConnectionSpy firstCreatedConnection = (RabbitMqConnectionSpy) rabbitFactorySpy.createdConnections
				.get(0);
		RabbitMqChannelSpy firstCreatedChannel = firstCreatedConnection.createdChannels.get(0);
		Map<String, Object> publishedMessage = firstCreatedChannel.publishedMessages.get(0);

		assertEquals(publishedMessage.get("body"), message.getBytes());

	}

}
