package se.uu.ub.cora.rabbitmq;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;

public class RabbitDeclareOkSpy implements DeclareOk {

	@Override
	public int protocolClassId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int protocolMethodId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String protocolMethodName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getQueue() {

		return "channelBinding";
	}

	@Override
	public int getMessageCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getConsumerCount() {
		// TODO Auto-generated method stub
		return 0;
	}

}
