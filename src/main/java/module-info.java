module se.uu.ub.cora.rabbitmq {
	requires se.uu.ub.cora.messaging;
	requires com.rabbitmq.client;
	requires transitive java.sql;

	provides se.uu.ub.cora.messaging.MessagingFactory
			with se.uu.ub.cora.rabbitmq.RabbitMqMessagingFactory;

}