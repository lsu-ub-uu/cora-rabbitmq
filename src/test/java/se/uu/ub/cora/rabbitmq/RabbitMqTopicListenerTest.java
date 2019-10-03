package se.uu.ub.cora.rabbitmq;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.messaging.MessageListener;
import se.uu.ub.cora.messaging.MessageRoutingInfo;

public class RabbitMqTopicListenerTest {

	private MessageRoutingInfo routingInfo;
	private RabbitMqConnectionFactorySpy rabbitFactorySpy;
	private RabbitMqTopicListener listener;

	@BeforeMethod
	public void beforeMethod() {
		rabbitFactorySpy = new RabbitMqConnectionFactorySpy();
		routingInfo = new MessageRoutingInfo("messaging.alvin-portal.org", "5672", "alvin", "index",
				"alvin.updates.#");
		listener = RabbitMqTopicListener
				.usingConnectionFactoryAndMessageRoutingInfo(rabbitFactorySpy, routingInfo);
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

	@Test
	public void testName() throws Exception {

	}

}
