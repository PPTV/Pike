package com.pplive.pike.exec.spout.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP.Queue;

import com.rabbitmq.client.Channel;

class SharedAutoDeleteQueueWithBinding implements QueueDeclaration {
	private static final long serialVersionUID = 1L;
	
	private final String queueName;
    private final String exchange;
    private final String routingKey;

    /**
     * Create a declaration of a named, non-durable, non-exclusive queue bound to
     * the specified exchange.
     *
     * @param queueName  name of the queue to be declared.
     * @param exchange  exchange to bind the queue to.
     * @param routingKey  routing key for the exchange binding.  Use "#" to
     *                    receive all messages published to the exchange.
     */
    public SharedAutoDeleteQueueWithBinding(String queueName, String exchange, String routingKey) {
        this.queueName = queueName;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    /**
     * Verifies the exchange exists, creates the named queue if it does not
     * exist, and binds it to the exchange.
     *
     * @return the server's response to the successful queue declaration.
     *
     * @throws IOException  if the exchange does not exist, the queue could not
     *                      be declared, or if the AMQP connection drops.
     */
    @Override
    public Queue.DeclareOk declare(Channel channel) throws IOException {
        channel.exchangeDeclarePassive(exchange);

        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-ha-policy", "all");
        final Queue.DeclareOk queue = channel.queueDeclare(
                queueName,
                /* non-durable */ false,
                /* non-exclusive */ false,
                /* auto-delete */ true,
                 args);

        channel.queueBind(queue.getQueue(), exchange, routingKey);

        return queue;
    }

    /**
     * Returns <tt>true</tt> as this queue is safe for parallel consumers.
     */
    @Override
    public boolean isParallelConsumable() {
        return true;
    }
}
