package se.uu.ub.cora.rabbitmq;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;

import se.uu.ub.cora.messaging.AmqpMessageRoutingInfo;
import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessagingInitializationException;
import se.uu.ub.cora.rabbitmq.spy.BasicPropertiesSpy;
import se.uu.ub.cora.rabbitmq.spy.MessageReceiverSpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqChannelSpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionFactorySpy;
import se.uu.ub.cora.rabbitmq.spy.RabbitMqConnectionSpy;

public class RabbitMqTopicListenerTest {

	private AmqpMessageRoutingInfo routingInfo;
	private RabbitMqConnectionFactorySpy rabbitConnectionFactorySpy;
	private RabbitMqTopicListener listener;

	@BeforeMethod
	public void beforeMethod() {
		rabbitConnectionFactorySpy = new RabbitMqConnectionFactorySpy();
		routingInfo = new AmqpMessageRoutingInfo("messaging.alvin-portal.org", "5672", "alvin",
				"index", "alvin.updates.#");
		listener = RabbitMqTopicListener.usingConnectionFactoryAndMessageRoutingInfo(
				rabbitConnectionFactorySpy, routingInfo);
	}

	@AfterMethod
	public void afterMethod() {
		rabbitConnectionFactorySpy = null;
		routingInfo = null;
		listener = null;
	}

	@Test
	public void testInit() throws Exception {
		assertSame(listener.getConnectionFactory(), rabbitConnectionFactorySpy);
		assertSame(listener.getMessageRoutingInfo(), routingInfo);

	}

	@Test
	public void testImplementsMessageListener() throws Exception {
		assertTrue(listener instanceof MessageListener);
	}

	@Test
	public void testSetConnectionFactoryChannel() throws Exception {
		int portAsInt = Integer.valueOf(routingInfo.port).intValue();

		listener.listen(null);

		assertEquals(rabbitConnectionFactorySpy.host, routingInfo.hostname);
		assertEquals(rabbitConnectionFactorySpy.port, portAsInt);
		assertEquals(rabbitConnectionFactorySpy.virtualHost, routingInfo.virtualHost);
	}

	@Test
	public void testConnectionsParametersAreSetBeforeNewConnectionIsCalled() throws Exception {
		int portAsInt = Integer.valueOf(routingInfo.port).intValue();
		listener.listen(null);

		RabbitMqConnectionSpy rabbitMqConnectionSpy = rabbitConnectionFactorySpy.createdConnections
				.get(0);
		assertEquals(rabbitMqConnectionSpy.host, routingInfo.hostname);
		assertEquals(rabbitMqConnectionSpy.port, portAsInt);
		assertEquals(rabbitMqConnectionSpy.virtualHost, routingInfo.virtualHost);
	}

	@Test
	public void testListenerCreatesAConnection() throws Exception {
		assertEquals(rabbitConnectionFactorySpy.createdConnections.size(), 0);
		listener.listen(null);
		assertEquals(rabbitConnectionFactorySpy.createdConnections.size(), 1);
	}

	@Test(expectedExceptions = MessagingInitializationException.class, expectedExceptionsMessageRegExp = ""
			+ "Error from RabbitMqConnectionFactorySpy on newConnection")
	public void testExceptionHandlingOnSendMessage() throws Exception {
		rabbitConnectionFactorySpy.throwErrorOnSendMessage = true;
		listener.listen(null);
	}

	@Test
	public void testExceptionHandlingOnSendMessageSendsAlongInitialException() throws Exception {
		rabbitConnectionFactorySpy.throwErrorOnSendMessage = true;
		try {
			listener.listen(null);
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof RuntimeException);
		}

	}

	@Test
	public void testListenMessageCreatesChannel() throws Exception {
		listener.listen(null);
		RabbitMqConnectionSpy firstCreatedConnection = getCreatedRabbitConnection(0);
		assertEquals(firstCreatedConnection.createdChannels.size(), 1);
	}

	@Test
	public void testBindingQueue() throws Exception {
		RabbitMqChannelSpy channelSpy = null;

		listener.listen(null);
		RabbitMqConnectionSpy firstCreatedConnection = getCreatedRabbitConnection(0);
		channelSpy = getRabbitChannel(0, firstCreatedConnection);

		assertEquals(channelSpy.queueBindings.size(), 1);
		assertEquals(channelSpy.queueBindings.get(0).get("queue"), "channelBinding");
		assertEquals(channelSpy.queueBindings.get(0).get("exchange"), routingInfo.exchange);
		assertEquals(channelSpy.queueBindings.get(0).get("routingKey"), routingInfo.routingKey);
	}

	@Test
	public void testBasicConsumeHasCorrectVAlues() throws Exception {
		RabbitMqChannelSpy channelSpy = null;

		listener.listen(null);
		RabbitMqConnectionSpy firstCreatedConnection = getCreatedRabbitConnection(0);
		channelSpy = getRabbitChannel(0, firstCreatedConnection);

		assertEquals(channelSpy.basicConsumes.size(), 1);
		assertEquals(channelSpy.basicConsumes.get(0).get("queue"), "channelBinding");
		assertEquals(channelSpy.basicConsumes.get(0).get("autoAck"), true);
		assertTrue(
				channelSpy.basicConsumes.get(0).get("deliverCallback") instanceof DeliverCallback);
		assertTrue(channelSpy.basicConsumes.get(0).get("cancelCallback") instanceof CancelCallback);
	}

	@Test
	public void testCallBacksAreHandledAndSentOnToReceiver() throws Exception {
		MessageReceiverSpy messageReceiverSpy = new MessageReceiverSpy();
		RabbitMqChannelSpy channelSpy = null;
		Delivery delivery = null;
		BasicPropertiesSpy properties = new BasicPropertiesSpy();
		Envelope envelope = null;
		Map<String, Object> headers = new HashMap<>();
		Map<String, String> headersReceived = new HashMap<>();

		headers.put("__TypeId__", "epc.messaging.amqp.EPCFedoraMessage");
		headers.put("ACTION", "UPDATE");
		headers.put("PID", "alvin-place:1");
		headers.put("messageSentFrom", "Cora");
		LongString longString = LongStringHelper.asLongString("notAStringValue");
		headers.put("notAString", longString);

		properties.setHeaders(headers);
		delivery = new Delivery(envelope, properties, "Łódź".getBytes(StandardCharsets.UTF_8));

		listener.listen(messageReceiverSpy);

		RabbitMqConnectionSpy firstCreatedConnection = getCreatedRabbitConnection(0);
		channelSpy = getRabbitChannel(0, firstCreatedConnection);
		DeliverCallback deliverCallback = (DeliverCallback) channelSpy.basicConsumes.get(0)
				.get("deliverCallback");
		deliverCallback.handle("consumerTag", delivery);

		assertEquals(messageReceiverSpy.messages.get(0), "Łódź");
		assertEquals(messageReceiverSpy.headers.size(), 1);

		headersReceived = messageReceiverSpy.headers.get(0);

		assertEquals(headersReceived.get("__TypeId__"), "epc.messaging.amqp.EPCFedoraMessage");
		assertEquals(headersReceived.get("ACTION"), "UPDATE");
		assertEquals(headersReceived.get("PID"), "alvin-place:1");
		assertEquals(headersReceived.get("messageSentFrom"), "Cora");
	}

	@Test
	public void testCallbackCancel() throws Exception {
		MessageReceiverSpy messageReceiverSpy = new MessageReceiverSpy();
		RabbitMqChannelSpy channelSpy = null;

		listener.listen(messageReceiverSpy);

		RabbitMqConnectionSpy firstCreatedConnection = getCreatedRabbitConnection(0);
		channelSpy = getRabbitChannel(0, firstCreatedConnection);

		CancelCallback cancelCallback = (CancelCallback) channelSpy.basicConsumes.get(0)
				.get("cancelCallback");
		cancelCallback.handle("consumerTag");

		assertTrue(messageReceiverSpy.topicClosedHasBeenCalled);
		RabbitMqConnectionSpy rabbitMqConnectionSpy = rabbitConnectionFactorySpy.createdConnections
				.get(0);
		RabbitMqChannelSpy rabbitMqChannelSpy = rabbitMqConnectionSpy.createdChannels.get(0);
		assertTrue(rabbitMqChannelSpy.closeHasBeenCalled);
		assertTrue(rabbitMqConnectionSpy.closeHasBeenCalled);
	}

	@Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ""
			+ "Error from RabbitMqConnectionSpy on close")
	public void testCallbackCancelErrorClosing() throws Exception {
		rabbitConnectionFactorySpy.throwErrorOnCloseConnection = true;
		MessageReceiverSpy messageReceiverSpy = new MessageReceiverSpy();
		RabbitMqChannelSpy channelSpy = null;

		listener.listen(messageReceiverSpy);

		RabbitMqConnectionSpy firstCreatedConnection = getCreatedRabbitConnection(0);
		channelSpy = getRabbitChannel(0, firstCreatedConnection);

		CancelCallback cancelCallback = (CancelCallback) channelSpy.basicConsumes.get(0)
				.get("cancelCallback");

		cancelCallback.handle("consumerTag");
	}

	@Test
	public void testCallbackCancelErrorClosingSendsAlongInitilException() throws Exception {
		rabbitConnectionFactorySpy.throwErrorOnCloseConnection = true;
		MessageReceiverSpy messageReceiverSpy = new MessageReceiverSpy();
		RabbitMqChannelSpy channelSpy = null;
		boolean exceptionWasThrown = false;

		try {
			listener.listen(messageReceiverSpy);

			RabbitMqConnectionSpy firstCreatedConnection = getCreatedRabbitConnection(0);
			channelSpy = getRabbitChannel(0, firstCreatedConnection);

			CancelCallback cancelCallback = (CancelCallback) channelSpy.basicConsumes.get(0)
					.get("cancelCallback");

			cancelCallback.handle("consumerTag");
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof RuntimeException);
			exceptionWasThrown = true;
		}
		assertTrue(exceptionWasThrown);
	}

	private RabbitMqConnectionSpy getCreatedRabbitConnection(int position) {
		return rabbitConnectionFactorySpy.createdConnections.get(position);
	}

	private RabbitMqChannelSpy getRabbitChannel(int position,
			RabbitMqConnectionSpy firstCreatedConnection) {
		return firstCreatedConnection.createdChannels.get(position);
	}
}
