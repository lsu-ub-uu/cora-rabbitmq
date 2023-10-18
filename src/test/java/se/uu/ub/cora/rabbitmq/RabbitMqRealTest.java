/*
 * Copyright 2023 Uppsala University Library
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

import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.messaging.AmqpMessageListenerRoutingInfo;
import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageReceiver;
import se.uu.ub.cora.messaging.MessageSender;

public class RabbitMqRealTest {

	private AmqpMessageListenerRoutingInfo routingInfo;
	private RabbitMqMessagingFactory factory;
	private MessageSender messageSender;
	private MessageListener messageListener;
	private MessageReceiver messageReceiver;
	private MessageReceiver messageReceiver2;
	private MessageListener messageListener2;

	@BeforeMethod
	public void beforeMethod() {
		factory = new RabbitMqMessagingFactory();
		String virtualHost = "/";
		String exchange = "workerE";
		String routingKey = "";
		routingInfo = new AmqpMessageListenerRoutingInfo("systemone-rabbitmq", "5672", virtualHost,
				exchange, routingKey);
		// routingInfo = new MessageRoutingInfo("systemone-rabbitmq", "5672", "/", "amq.direct",
		// "alvin.updates.#");
		// routingInfo = new MessageRoutingInfo("systemone-rabbitmq", "5672", "routingKey");

		messageSender = factory.factorTopicMessageSender(routingInfo);
		messageListener = factory.factorTopicMessageListener(routingInfo);
		messageListener2 = factory.factorTopicMessageListener(routingInfo);
		messageReceiver = new MessageReceiver() {
			@Override
			public void topicClosed() {
				// TODO Auto-generated method stub

				System.out.println("Topic closed... ");
			}

			@Override
			public void receiveMessage(Map<String, String> headers, String message) {
				// TODO Auto-generated method stub
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("worker_SLOW: h:" + headers + " m:" + message);
			}
		};
		messageReceiver2 = new MessageReceiver() {
			@Override
			public void topicClosed() {
				// TODO Auto-generated method stub

				System.out.println("Topic closed... ");
			}

			@Override
			public void receiveMessage(Map<String, String> headers, String message) {
				// TODO Auto-generated method stub
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("worker_FAST: h:" + headers + " m:" + message);
			}
		};
	}

	@Test
	public void testImplementsMessageSender() throws Exception {
		assertTrue(messageSender instanceof MessageSender);
		System.out.println("adsf");

		HashMap<String, Object> headers = new HashMap<>();
		headers.put("id", "binary:3232");
		headers.put("type", "type");
		messageSender.sendMessage(headers, "hej pere1A");
		messageSender.sendMessage(headers, "hej pere2A");
		messageSender.sendMessage(headers, "hej pere3A");
		messageListener.listen(messageReceiver2);
		messageListener.listen(messageReceiver);
		messageSender.sendMessage(headers, "hej pere4A");

		Thread.sleep(10000);
		System.out.println("slept 1");
		// Thread.sleep(1000);
		// System.out.println("slept 2");

	}

}
