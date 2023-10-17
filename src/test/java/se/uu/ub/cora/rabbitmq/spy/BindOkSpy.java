package se.uu.ub.cora.rabbitmq.spy;

import com.rabbitmq.client.AMQP.Exchange.BindOk;

public class BindOkSpy implements BindOk {

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

}
