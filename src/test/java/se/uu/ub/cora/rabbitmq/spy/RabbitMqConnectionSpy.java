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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;

public class RabbitMqConnectionSpy implements Connection {

	public List<RabbitMqChannelSpy> createdChannels = new ArrayList<RabbitMqChannelSpy>();
	public boolean closeHasBeenCalled = false;
	public String host;
	public int port;
	public String virtualHost;

	@Override
	public void addShutdownListener(ShutdownListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeShutdownListener(ShutdownListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public ShutdownSignalException getCloseReason() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void notifyListeners() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public InetAddress getAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getChannelMax() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFrameMax() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHeartbeat() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, Object> getClientProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientProvidedName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getServerProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Channel createChannel() throws IOException {
		RabbitMqChannelSpy rabbitChannel = new RabbitMqChannelSpy();

		createdChannels.add(rabbitChannel);
		return rabbitChannel;
	}

	@Override
	public Channel createChannel(int channelNumber) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		closeHasBeenCalled = true;
	}

	@Override
	public void close(int closeCode, String closeMessage) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close(int timeout) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close(int closeCode, String closeMessage, int timeout) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort() {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort(int closeCode, String closeMessage) {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort(int timeout) {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort(int closeCode, String closeMessage, int timeout) {
		// TODO Auto-generated method stub

	}

	@Override
	public void addBlockedListener(BlockedListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public BlockedListener addBlockedListener(BlockedCallback blockedCallback,
			UnblockedCallback unblockedCallback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean removeBlockedListener(BlockedListener listener) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearBlockedListeners() {
		// TODO Auto-generated method stub

	}

	@Override
	public ExceptionHandler getExceptionHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setId(String id) {
		// TODO Auto-generated method stub

	}

}
