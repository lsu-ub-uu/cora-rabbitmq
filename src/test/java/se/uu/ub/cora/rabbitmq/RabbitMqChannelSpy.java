package se.uu.ub.cora.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Basic.RecoverOk;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Exchange.BindOk;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.AMQP.Exchange.UnbindOk;
import com.rabbitmq.client.AMQP.Queue.PurgeOk;
import com.rabbitmq.client.AMQP.Tx.CommitOk;
import com.rabbitmq.client.AMQP.Tx.RollbackOk;
import com.rabbitmq.client.AMQP.Tx.SelectOk;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMqChannelSpy implements Channel {

	public List<Map<String, Object>> publishedMessages = new ArrayList<>();

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
	public int getChannelNumber() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Connection getConnection() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException, TimeoutException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close(int closeCode, String closeMessage) throws IOException, TimeoutException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void abort(int closeCode, String closeMessage) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addReturnListener(ReturnListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public ReturnListener addReturnListener(ReturnCallback returnCallback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean removeReturnListener(ReturnListener listener) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearReturnListeners() {
		// TODO Auto-generated method stub

	}

	@Override
	public void addConfirmListener(ConfirmListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public ConfirmListener addConfirmListener(ConfirmCallback ackCallback,
			ConfirmCallback nackCallback) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean removeConfirmListener(ConfirmListener listener) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearConfirmListeners() {
		// TODO Auto-generated method stub

	}

	@Override
	public Consumer getDefaultConsumer() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setDefaultConsumer(Consumer consumer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void basicQos(int prefetchCount, boolean global) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void basicQos(int prefetchCount) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body)
			throws IOException {
		Map<String, Object> publishedMessage = new HashMap<>();
		publishedMessage.put("body", body);
		publishedMessages.add(publishedMessage);
	}

	@Override
	public void basicPublish(String exchange, String routingKey, boolean mandatory,
			BasicProperties props, byte[] body) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void basicPublish(String exchange, String routingKey, boolean mandatory,
			boolean immediate, BasicProperties props, byte[] body) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type, boolean durable)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type, boolean durable,
			boolean autoDelete, Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable,
			boolean autoDelete, Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, String type, boolean durable,
			boolean autoDelete, boolean internal, Map<String, Object> arguments)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable,
			boolean autoDelete, boolean internal, Map<String, Object> arguments)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void exchangeDeclareNoWait(String exchange, String type, boolean durable,
			boolean autoDelete, boolean internal, Map<String, Object> arguments)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable,
			boolean autoDelete, boolean internal, Map<String, Object> arguments)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public DeclareOk exchangeDeclarePassive(String name) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public DeleteOk exchangeDelete(String exchange) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BindOk exchangeBind(String destination, String source, String routingKey)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BindOk exchangeBind(String destination, String source, String routingKey,
			Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void exchangeBindNoWait(String destination, String source, String routingKey,
			Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public UnbindOk exchangeUnbind(String destination, String source, String routingKey)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UnbindOk exchangeUnbind(String destination, String source, String routingKey,
			Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void exchangeUnbindNoWait(String destination, String source, String routingKey,
			Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable,
			boolean exclusive, boolean autoDelete, Map<String, Object> arguments)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive,
			boolean autoDelete, Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclarePassive(String queue)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused,
			boolean ifEmpty) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue, String exchange,
			String routingKey) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.BindOk queueBind(String queue, String exchange,
			String routingKey, Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void queueBindNoWait(String queue, String exchange, String routingKey,
			Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange,
			String routingKey) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.rabbitmq.client.AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange,
			String routingKey, Map<String, Object> arguments) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PurgeOk queuePurge(String queue) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void basicAck(long deliveryTag, boolean multiple) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void basicReject(long deliveryTag, boolean requeue) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String basicConsume(String queue, Consumer callback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, DeliverCallback deliverCallback,
			CancelCallback cancelCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, DeliverCallback deliverCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, DeliverCallback deliverCallback,
			CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, Consumer callback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
			CancelCallback cancelCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback,
			CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
			Consumer callback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
			DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
			DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments,
			DeliverCallback deliverCallback, CancelCallback cancelCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag,
			DeliverCallback deliverCallback, CancelCallback cancelCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag,
			DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag,
			DeliverCallback deliverCallback, CancelCallback cancelCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
			boolean exclusive, Map<String, Object> arguments, Consumer callback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
			boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
			CancelCallback cancelCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
			boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
			ConsumerShutdownSignalCallback shutdownSignalCallback) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal,
			boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback,
			CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void basicCancel(String consumerTag) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public RecoverOk basicRecover() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecoverOk basicRecover(boolean requeue) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SelectOk txSelect() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CommitOk txCommit() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RollbackOk txRollback() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public com.rabbitmq.client.AMQP.Confirm.SelectOk confirmSelect() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNextPublishSeqNo() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean waitForConfirms() throws InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void waitForConfirmsOrDie() throws IOException, InterruptedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void waitForConfirmsOrDie(long timeout)
			throws IOException, InterruptedException, TimeoutException {
		// TODO Auto-generated method stub

	}

	@Override
	public void asyncRpc(Method method) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Command rpc(Method method) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long messageCount(String queue) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long consumerCount(String queue) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CompletableFuture<Command> asyncCompletableRpc(Method method) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
