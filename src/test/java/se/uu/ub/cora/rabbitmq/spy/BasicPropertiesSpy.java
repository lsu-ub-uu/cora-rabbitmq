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

package se.uu.ub.cora.rabbitmq.spy;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.AMQBasicProperties;
import com.rabbitmq.client.impl.ContentHeaderPropertyWriter;

public class BasicPropertiesSpy extends AMQP.BasicProperties {

	private Map<String, Object> headers;

	@Override
	public String getContentType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getContentEncoding() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getHeaders() {
		return headers;
	}

	@Override
	public Integer getDeliveryMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getPriority() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCorrelationId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getReplyTo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getExpiration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMessageId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getTimestamp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getUserId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getAppId() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setHeaders(Map<String, Object> headers) {
		this.headers = headers;

	}

	@Override
	public int getClassId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getClassName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void writePropertiesTo(ContentHeaderPropertyWriter writer) throws IOException {
		// TODO Auto-generated method stub

	}

	public AMQBasicProperties toAMQBasicProperties() {
		return this;
	}

}
