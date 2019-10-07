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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqConnectionFactorySpy extends ConnectionFactory {

	public String host;
	public int port;
	public String virtualHost;
	public List<RabbitMqConnectionSpy> createdConnections = new ArrayList<>();
	public boolean throwErrorOnSendMessage = false;

	@Override
	public void setHost(String host) {
		this.host = host;
	}

	@Override
	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void setVirtualHost(String virtualHost) {
		this.virtualHost = virtualHost;
	}

	@Override
	public Connection newConnection() throws IOException, TimeoutException {
		if (throwErrorOnSendMessage) {
			throw new RuntimeException("Error from RabbitMqConnectionFactorySpy on newConnection");
		}
		RabbitMqConnectionSpy rabbitMqConnectionSpy = new RabbitMqConnectionSpy();
		createdConnections.add(rabbitMqConnectionSpy);
		return rabbitMqConnectionSpy;
	}
}
