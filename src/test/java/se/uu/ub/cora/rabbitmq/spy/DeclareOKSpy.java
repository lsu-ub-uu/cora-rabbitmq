/*
 * Copyright 2023, 2025 Uppsala University Library
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

package se.uu.ub.cora.rabbitmq.spy;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;

import se.uu.ub.cora.testutils.mcr.MethodCallRecorder;
import se.uu.ub.cora.testutils.mrv.MethodReturnValues;

public class DeclareOKSpy implements DeclareOk {
	public MethodCallRecorder MCR = new MethodCallRecorder();
	public MethodReturnValues MRV = new MethodReturnValues();

	public DeclareOKSpy() {
		MCR.useMRV(MRV);
		MRV.setDefaultReturnValuesSupplier("getQueue", () -> "someQueueName");
	}

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
		return (String) MCR.addCallAndReturnFromMRV();
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
