package fi.aalto.itia.aggregator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import fi.aalto.itia.adr_em_common.ADR_EM_Common;
import fi.aalto.itia.adr_em_common.SimulationElement;
import fi.aalto.itia.adr_em_common.SimulationMessage;
import fi.aalto.itia.adr_em_common.SimulationMessageFactory;
import fi.aalto.itia.adr_em_common.UpdateMessageContent;
import fi.aalto.itia.adr_em_common.comparators.UpdateSortByTimeCutComparator;

/**
 * @author giovanc1
 *
 */
public class Aggregator extends SimulationElement {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5676710132282560560L;
	private static final Logger log = Logger.getLogger(Aggregator.class);
	private static Aggregator agg;

	// Keeps track of registered consumers
	private Set<String> consumers;
	// map that keeps ordered by key (ConsumerIDQUEUE) //TODO you can get a list
	// out of it and order it with Comparators
	private TreeMap<String, UpdateMessageContent> consumersUpdates;

	private Aggregator(String inputQueueName) {
		super(inputQueueName);
		consumers = new LinkedHashSet<String>();
		consumersUpdates = new TreeMap<String, UpdateMessageContent>();
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
		/*
		 * while (keepGoing) { /*SimulationMessage sm = this.pollMessageMs(1);
		 * if (sm != null) { i++; log.debug(i + " MESSAGE " + sm.toString()); }
		 * else { try { Thread.sleep(15000); } catch (InterruptedException e) {
		 * e.printStackTrace(); } } }
		 */
		try {
			Thread.sleep(1000 * 15);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO
		List<UpdateMessageContent> collectionUpdate = new ArrayList<UpdateMessageContent>(
				consumersUpdates.values());
		Collections.sort(collectionUpdate,
				UpdateMessageContent.SortByTimeCutComparator);
		for (UpdateMessageContent updateMessageContent : collectionUpdate) {
			System.out.println("ORDERED: " + updateMessageContent.toString());
		}
		for (UpdateMessageContent updateMessageContent : collectionUpdate) {
			System.out.println("ORDERED_BY_TIME_CUT: " + updateMessageContent.getTimeCut());
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
		log.info(sm.toString());
		switch (sm.getHeader()) {
		case ADR_EM_Common.REG_HEADER:
			// add the consumer to the set
			addConsumer(sm);
			// Add the update
			addToConsumersUpdates(sm.getContent());
			break;
		case ADR_EM_Common.STATUS_UPDATE_HEADER:
			// if the consumer is already registered
			if (consumers.contains(sm.getSender()))
				addToConsumersUpdates(sm.getContent());
			break;
		default:
			addMessage(sm);
			break;
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

	public TreeMap<String, UpdateMessageContent> getConsumersUpdates() {
		return consumersUpdates;
	}

	public void setConsumersUpdates(
			TreeMap<String, UpdateMessageContent> consumersUpdates) {
		this.consumersUpdates = consumersUpdates;
	}

	public synchronized void addToConsumersUpdates(Serializable umc) {
		// if the content of the message is UpdateMessageContent
		if (umc instanceof UpdateMessageContent) {
			UpdateMessageContent uMsg = (UpdateMessageContent) umc;
			// Consumer Identifier
			String consumerIoQueue = uMsg.getConsumerSender();
			// Overwrite with the last update
			consumersUpdates.put(consumerIoQueue, uMsg);
		} else {
			log.debug("REGISTRATION SENt WITHOUT UPDATE CONTENT");
		}
	}

}