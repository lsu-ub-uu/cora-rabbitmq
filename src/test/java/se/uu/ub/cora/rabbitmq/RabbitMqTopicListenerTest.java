/*
 * Copyright 2019, 2023, 2025 Uppsala University Library
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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;

import se.uu.ub.cora.logger.LoggerProvider;
import se.uu.ub.cora.logger.spies.LoggerFactorySpy;
import se.uu.ub.cora.logger.spies.LoggerSpy;
import se.uu.ub.cora.messaging.AmqpMessageListenerRoutingInfo;
import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessagingInitializationException;
import se.uu.ub.cora.rabbitmq.spy.BasicPropertiesSpy;
import se.uu.ub.cora.rabbitmq.spy.DeclareOKSpy;
import se.uu.ub.cora.rabbitmq.spy.EnvelopeSpy;
import se.uu.ub.cora.rabbitmq.spy.MessageReceiverSpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqChannelSpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionFactorySpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionSpy;

public class RabbitMqTopicListenerTest {

	private static final String ERROR_LISTENING = "Error trying to listen rabbitMQ queue on "
			+ "{0}:{1}, vhost: {2} and queue: {3}";
	private AmqpMessageListenerRoutingInfo routingInfo;
	private RabbitMqConnectionFactorySpy rabbitConnectionFactorySpy;
	private RabbitMqTopicListener listener;

	private static final String SOME_HOST = "someHostname";
	private static final int SOME_PORT = 8080;
	private static final String SOME_VHOST = "someVirtualHost";
	private static final String SOME_QUEUE = "someQueue";
	private MessageReceiverSpy messageReceiverSpy;
	private LoggerFactorySpy loggerFactory;

	@BeforeMethod
	public void beforeMethod() {
		loggerFactory = new LoggerFactorySpy();
		LoggerProvider.setLoggerFactory(loggerFactory);

		rabbitConnectionFactorySpy = new RabbitMqConnectionFactorySpy();
		routingInfo = new AmqpMessageListenerRoutingInfo(SOME_HOST, SOME_PORT, SOME_VHOST,
				SOME_QUEUE);
		listener = RabbitMqTopicListener.usingConnectionFactoryAndMessageRoutingInfo(
				rabbitConnectionFactorySpy, routingInfo);

		messageReceiverSpy = new MessageReceiverSpy();
	}

	@Test
	public void testInit() {
		assertSame(listener.onlyForTestGetConnectionFactory(), rabbitConnectionFactorySpy);
		assertSame(listener.onlyForTestGetMessageRoutingInfo(), routingInfo);
	}

	@Test
	public void testImplementsMessageListener() {
		assertTrue(listener instanceof MessageListener);
	}

	@Test
	public void testSetConnectionFactoryChannel() {

		listener.listen(messageReceiverSpy);

		rabbitConnectionFactorySpy.MCR.assertParameters("setHost", 0, SOME_HOST);
		rabbitConnectionFactorySpy.MCR.assertParameters("setPort", 0, SOME_PORT);
		rabbitConnectionFactorySpy.MCR.assertParameters("setVirtualHost", 0, SOME_VHOST);
	}

	@Test
	public void testListenerCreatesAConnection() {
		rabbitConnectionFactorySpy.MCR.assertNumberOfCallsToMethod("newConnection", 0);
		listener.listen(messageReceiverSpy);
		rabbitConnectionFactorySpy.MCR.assertNumberOfCallsToMethod("newConnection", 1);
	}

	@Test
	public void testExceptionHandlingOnNewConnection() {
		String errorMessageSpy = "Error from RabbitMqConnectionFactorySpy on newConnection";
		RuntimeException returnException = new RuntimeException(errorMessageSpy);
		rabbitConnectionFactorySpy.MRV.setAlwaysThrowException("newConnection", returnException);

		try {
			listener.listen(messageReceiverSpy);
			fail("Should have thrown an exception");
		} catch (Exception e) {
			assertTrue(e instanceof MessagingInitializationException);
			assertEquals(e.getMessage(), MessageFormat.format(ERROR_LISTENING, SOME_HOST,
					String.valueOf(SOME_PORT), SOME_VHOST, SOME_QUEUE));
			assertEquals(e.getCause().getMessage(), errorMessageSpy);
		}
	}

	@Test
	public void testListenMessageCreatesChannel() {
		listener.listen(messageReceiverSpy);

		RabbitMqConnectionSpy connection = getConnection();

		connection.MCR.assertNumberOfCallsToMethod("createChannel", 1);
	}

	private RabbitMqConnectionSpy getConnection() {
		return (RabbitMqConnectionSpy) rabbitConnectionFactorySpy.MCR
				.getReturnValue("newConnection", 0);
	}

	private RabbitMqChannelSpy getChannel() {
		RabbitMqConnectionSpy connection = getConnection();
		return (RabbitMqChannelSpy) connection.MCR.getReturnValue("createChannel", 0);
	}

	@Test
	public void testBasicConsumeHasCorrectVAlues() {

		listener.listen(messageReceiverSpy);

		RabbitMqChannelSpy channel = getChannel();

		boolean autoAck = false;
		channel.MCR.assertParameters("basicConsume", 0, SOME_QUEUE, autoAck);
	}

	@Test
	public void testCallBacksAreHandledAndSentOnToReceiver() throws IOException {
		Map<String, Object> headers = new HashMap<>();
		headers.put("__TypeId__", "epc.messaging.amqp.EPCFedoraMessage");
		headers.put("ACTION", "UPDATE");
		headers.put("PID", "alvin-place:1");
		headers.put("messageSentFrom", "Cora");
		LongString longString = LongStringHelper.asLongString("notAStringValue");
		headers.put("notAString", longString);

		BasicPropertiesSpy properties = new BasicPropertiesSpy();
		properties.setHeaders(headers);
		EnvelopeSpy envelope = new EnvelopeSpy();
		Delivery delivery = new Delivery(envelope, properties,
				"Łódź".getBytes(StandardCharsets.UTF_8));

		listener.listen(messageReceiverSpy);

		RabbitMqChannelSpy channelSpy = getChannel();
		DeliverCallback deliverCallback = getDeliverCallback(channelSpy);

		callToDeliverCallback(delivery, deliverCallback);

		messageReceiverSpy.MCR.assertParameter("receiveMessage", 0, "message", "Łódź");

		Map<String, String> headersReceived = (Map<String, String>) messageReceiverSpy.MCR
				.getParameterForMethodAndCallNumberAndParameter("receiveMessage", 0, "headers");
		assertEquals(headersReceived.get("__TypeId__"), "epc.messaging.amqp.EPCFedoraMessage");
		assertEquals(headersReceived.get("ACTION"), "UPDATE");
		assertEquals(headersReceived.get("PID"), "alvin-place:1");
		assertEquals(headersReceived.get("messageSentFrom"), "Cora");
	}

	private DeliverCallback getDeliverCallback(RabbitMqChannelSpy channelSpy) {
		return (DeliverCallback) channelSpy.MCR.getParameterForMethodAndCallNumberAndParameter(
				"basicConsume", 0, "deliverCallback");
	}

	@Test
	public void testAcknowledeMessage() throws IOException {
		listener.listen(messageReceiverSpy);

		BasicPropertiesSpy properties = new BasicPropertiesSpy();
		properties.setHeaders(Collections.emptyMap());
		EnvelopeSpy envelope = new EnvelopeSpy();
		Delivery delivery = new Delivery(envelope, properties,
				"Łódź".getBytes(StandardCharsets.UTF_8));

		RabbitMqChannelSpy channelSpy = getChannel();
		DeliverCallback deliverCallback = getDeliverCallback(channelSpy);

		callToDeliverCallback(delivery, deliverCallback);

		long deliveryTagFromSpy = (long) envelope.MCR.getReturnValue("getDeliveryTag", 0);
		channelSpy.MCR.assertParameters("basicAck", 0, deliveryTagFromSpy, false);
	}

	@Test
	public void testCallbackCancel() throws IOException {
		listener.listen(messageReceiverSpy);

		RabbitMqChannelSpy channel = getChannel();
		CancelCallback cancelCallback = getCancelCallback(channel);

		cancelCallback.handle("consumerTag");

		messageReceiverSpy.MCR.assertMethodWasCalled("topicClosed");
		channel.MCR.assertMethodWasCalled("close");
		RabbitMqConnectionSpy connection = getConnection();
		connection.MCR.assertMethodWasCalled("close");
	}

	private CancelCallback getCancelCallback(RabbitMqChannelSpy channel) {
		return (CancelCallback) channel.MCR.getParameterForMethodAndCallNumberAndParameter(
				"basicConsume", 0, "cancelCallback");
	}

	@Test
	public void testCallbackCancelErrorClosing() {
		RabbitMqConnectionSpy connectionSpy = new RabbitMqConnectionSpy();
		RuntimeException errorToThrow = new RuntimeException(
				"Error from RabbitMqConnectionSpy on close");
		connectionSpy.MRV.setAlwaysThrowException("close", errorToThrow);
		rabbitConnectionFactorySpy.MRV.setDefaultReturnValuesSupplier("newConnection",
				() -> connectionSpy);

		try {
			listener.listen(messageReceiverSpy);

			RabbitMqChannelSpy channel = getChannel();
			CancelCallback cancelCallback = getCancelCallback(channel);
			cancelCallback.handle("consumerTag");

			Assert.fail("Error should have been thrown");
		} catch (Exception e) {
			assertEquals(e.getMessage(),
					"Error closing channel and connection in RabbitMqTopicListener");
			assertSame(e.getCause(), errorToThrow);

		}
	}

	@Test
	public void testListenAutoCreateQueues() {
		routingInfo = new AmqpMessageListenerRoutingInfo(SOME_HOST, SOME_PORT, SOME_VHOST,
				"someExchange", "someRoutingKey");
		listener = RabbitMqTopicListener.usingConnectionFactoryAndMessageRoutingInfo(
				rabbitConnectionFactorySpy, routingInfo);

		listener.listen(messageReceiverSpy);

		RabbitMqChannelSpy channel = getChannel();
		var queueDeclare = (DeclareOKSpy) channel.MCR.assertCalledParametersReturn("queueDeclare");
		var queueName = (String) queueDeclare.MCR.assertCalledParametersReturn("getQueue");
		channel.MCR.assertParameters("queueBind", 0, queueName, "someExchange", "someRoutingKey");
		channel.MCR.assertParameters("basicConsume", 0, queueName, false,
				getDeliverCallback(channel), getCancelCallback(channel));

		LoggerSpy logger = getLogger(1);
		logger.MCR.assertParameters("logInfoUsingMessage", 0,
				"Binding queue: someQueueName using routingKey: someRoutingKey to exchange: someExchange");
	}

	private LoggerSpy getLogger(int callNumber) {
		return (LoggerSpy) loggerFactory.MCR.getReturnValue("factorForClass", callNumber);
	}

	@Test
	public void testUnableToDeclareQueue() {
		RabbitMqChannelSpy channelSpy = setUpChannelSpy();
		channelSpy.MRV.setDefaultReturnValuesSupplier("queueDeclare", IOException::new);

		routingInfo = new AmqpMessageListenerRoutingInfo(SOME_HOST, SOME_PORT, SOME_VHOST,
				"someExchange", "someRoutingKey");
		listener = RabbitMqTopicListener.usingConnectionFactoryAndMessageRoutingInfo(
				rabbitConnectionFactorySpy, routingInfo);

		try {
			listener.listen(messageReceiverSpy);
			fail();
		} catch (Exception e) {
			assertTrue(e instanceof MessagingInitializationException);
			assertEquals(e.getMessage(),
					"Error trying to listen rabbitMQ queue on someHostname:8080, vhost: "
							+ "someVirtualHost, exchange: someExchange and routingKey: someRoutingKey");
		}
	}

	private RabbitMqChannelSpy setUpChannelSpy() {
		RabbitMqConnectionSpy connectionSpy = new RabbitMqConnectionSpy();
		rabbitConnectionFactorySpy.MRV.setDefaultReturnValuesSupplier("newConnection",
				() -> connectionSpy);
		RabbitMqChannelSpy channelSpy = new RabbitMqChannelSpy();
		connectionSpy.MRV.setDefaultReturnValuesSupplier("createChannel", () -> channelSpy);
		return channelSpy;
	}

	private void callToDeliverCallback(Delivery delivery, DeliverCallback deliverCallback)
			throws IOException {
		deliverCallback.handle("consumerTag", delivery);
	}
}
