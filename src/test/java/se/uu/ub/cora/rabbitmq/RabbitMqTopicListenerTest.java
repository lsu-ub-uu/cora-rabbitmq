package se.uu.ub.cora.rabbitmq;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageRoutingInfo;
import se.uu.ub.cora.messaging.MessagingInitializationException;

public class RabbitMqTopicListenerTest {

	private MessageRoutingInfo routingInfo;
	private RabbitMqConnectionFactorySpy rabbitFactorySpy;
	private RabbitMqTopicListener listener;
	private RabbitMqConnectionSpy firstCreatedConnection;

	@BeforeMethod
	public void beforeMethod() {
		rabbitFactorySpy = new RabbitMqConnectionFactorySpy();
		routingInfo = new MessageRoutingInfo("messaging.alvin-portal.org", "5672", "alvin", "index",
				"alvin.updates.#");
		listener = RabbitMqTopicListener
				.usingConnectionFactoryAndMessageRoutingInfo(rabbitFactorySpy, routingInfo);
		firstCreatedConnection = null;
	}

	@Test
	public void testInit() throws Exception {
		assertSame(listener.getConnectionFactory(), rabbitFactorySpy);
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

		assertEquals(rabbitFactorySpy.host, routingInfo.hostname);
		assertEquals(rabbitFactorySpy.port, portAsInt);
		assertEquals(rabbitFactorySpy.virtualHost, routingInfo.virtualHost);
	}

	@Test
	public void testListenerCreatesAConnection() throws Exception {
		assertEquals(rabbitFactorySpy.createdConnections.size(), 0);
		listener.listen(null);
		assertEquals(rabbitFactorySpy.createdConnections.size(), 1);
	}

	@Test(expectedExceptions = MessagingInitializationException.class, expectedExceptionsMessageRegExp = ""
			+ "Error from RabbitMqConnectionFactorySpy on newConnection")
	public void testExceptionHandlingOnSendMessage() throws Exception {
		rabbitFactorySpy.throwErrorOnSendMessage = true;
		listener.listen(null);
	}

	@Test
	public void testListenMessageCreatesChannel() throws Exception {
		listener.listen(null);
		firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		assertEquals(firstCreatedConnection.createdChannels.size(), 1);
	}

	@Test
	public void testBindingQueue() throws Exception {
		listener.listen(null);
		firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		RabbitMqChannelSpy channelSpy = firstCreatedConnection.createdChannels.get(0);

		assertEquals(channelSpy.queueBindings.size(), 1);
		assertEquals(channelSpy.queueBindings.get(0).get("queue"), "channelBinding");
		assertEquals(channelSpy.queueBindings.get(0).get("exchange"), routingInfo.exchange);
		assertEquals(channelSpy.queueBindings.get(0).get("routingKey"), routingInfo.routingKey);
	}

	@Test
	public void testBasicConsumeHasCorrectVAlues() throws Exception {
		listener.listen(null);
		firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		RabbitMqChannelSpy channelSpy = firstCreatedConnection.createdChannels.get(0);

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
		listener.listen(messageReceiverSpy);
		firstCreatedConnection = rabbitFactorySpy.createdConnections.get(0);
		RabbitMqChannelSpy channelSpy = firstCreatedConnection.createdChannels.get(0);
		DeliverCallback deliverCallback = (DeliverCallback) channelSpy.basicConsumes.get(0)
				.get("deliverCallback");
		Delivery delivery = new Delivery(null, null, "Łódź".getBytes(StandardCharsets.UTF_8));
		deliverCallback.handle("consumerTag", delivery);
		assertEquals(messageReceiverSpy.messages.get(0), "Łódź");
	}
}
