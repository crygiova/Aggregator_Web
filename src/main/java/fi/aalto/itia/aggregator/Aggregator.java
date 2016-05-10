package fi.aalto.itia.aggregator;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import fi.aalto.itia.adr_em_common.ADR_EM_Common;
import fi.aalto.itia.adr_em_common.SimulationElement;
import fi.aalto.itia.adr_em_common.SimulationMessage;
import fi.aalto.itia.adr_em_common.SimulationMessageFactory;

/**
 * @author giovanc1
 *
 */
public class Aggregator extends SimulationElement {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5676710132282560560L;
	private Set<String> consumers;
	private static final Logger log = Logger.getLogger(Aggregator.class);
	private static Aggregator agg;

	private Aggregator(String inputQueueName) {
		super(inputQueueName);
		consumers = new LinkedHashSet<String>();
	}

	// Singleton implementation,
	public static Aggregator getInstance() {
		return agg;
	}

	// New instance of aggregator
	public static Aggregator getNewInstance(String inputQueue) {
		agg = new Aggregator(inputQueue);
		return agg;
	}

	@Override
	public void run() {
		this.startConsumingMq();
		int i = 0;
		log.debug("Start Aggregator");
		while (keepGoing) {
			SimulationMessage sm = this.pollMessageMs(1);
			if (sm != null) {
				i++;
				log.debug(i + " MESSAGE " + sm.toString());
			} else {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		this.closeConnection();
		log.debug("End of Aggregator");
	}


	// TODO change this one when there is a message coming in for each actor
	public void startConsumingMq() {
		Consumer consumer = new DefaultConsumer(dRChannel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					AMQP.BasicProperties properties, byte[] body)
					throws IOException {
				SimulationMessage sm = null;
				try {
					sm = (SimulationMessage) SimulationMessage
							.deserialize(body);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				if (sm != null) {
					routeInputMessage(sm);
				}
			}
		};
		try {
			dRChannel.basicConsume(inputQueueName, true, consumer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// This function routes the input messages based on their headers
	public void routeInputMessage(SimulationMessage sm) {
		System.out.println(sm.toString());
		if (sm.getHeader().compareTo(ADR_EM_Common.REG_HEADER) == 0) {
			addConsumer(sm);
		} else {
			// TODO if there is priority put in a priority queue otherwise all
			// in the message queue
			addMessage(sm);
		}
	}

	// Consumer Registration to the DR system
	public void addConsumer(SimulationMessage registrationMsg) {

		if (!consumers.contains(registrationMsg.getSender())) {
			consumers.add(registrationMsg.getSender());
			log.debug("Consumer Registered");
			this.sendMessage(SimulationMessageFactory.getRegisterAccept(
					inputQueueName, registrationMsg.getSender()));
		} else {
			log.debug("Consumer ALREADY Registered");
			this.sendMessage(SimulationMessageFactory.getRegisterDeny(
					inputQueueName, registrationMsg.getSender()));
		}
	}

	public int getConsumersSize() {
		return consumers.size();
	}

	@Override
	public void scheduleTasks() {
		// TODO Auto-generated method stub

	}

	@Override
	public void executeTasks() {
		// TODO Auto-generated method stub

	}

	@Override
	public void elaborateIncomingMessages() {
		// TODO Auto-generated method stub

	}

}