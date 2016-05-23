package fi.aalto.itia.aggregator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import fi.aalto.itia.adr_em_common.ADR_EM_Common;
import fi.aalto.itia.adr_em_common.InstructionsMessageContent;
import fi.aalto.itia.adr_em_common.SimulationElement;
import fi.aalto.itia.adr_em_common.SimulationMessage;
import fi.aalto.itia.adr_em_common.SimulationMessageFactory;
import fi.aalto.itia.adr_em_common.UpdateMessageContent;
import fi.aalto.itia.util.Utility;

/**
 * @author giovanc1
 *
 */
public class Aggregator extends SimulationElement {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5676710132282560560L;
	private static final String FILE_NAME_PROPERTIES = "agg_config.properties";
	private static final String TARGET_FLEX = "TARGET_FLEX";
	private static final String FREQ_BAND = "FREQ_BAND";
	private static final double MAX_FCRN_FREQ_VARIATION = 0.1d;
	private static final double NOMINAL_FREQ = 50d;
	private static final double BOTTOM_FREQ = NOMINAL_FREQ
			- MAX_FCRN_FREQ_VARIATION;
	private static final double TOP_FREQ = NOMINAL_FREQ
			+ MAX_FCRN_FREQ_VARIATION;

	private static final Logger log = Logger.getLogger(Aggregator.class);
	private static Aggregator agg;
	private static final double targetFlex;
	private static final double freqDeadBand;

	// Keeps track of registered consumers
	private Set<String> consumers;
	// map that keeps ordered by key (ConsumerIDQUEUE) //TODO you can get a list
	// out of it and order it with Comparators
	private TreeMap<String, UpdateMessageContent> consumersUpdates;

	static {
		Properties properties = Utility.getProperties(FILE_NAME_PROPERTIES);
		targetFlex = Double.parseDouble(properties.getProperty(TARGET_FLEX));
		freqDeadBand = Double.parseDouble(properties.getProperty(FREQ_BAND));
	}

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
		log.debug("Start Aggregator");
		/*
		 * while (keepGoing) { /*SimulationMessage sm = this.pollMessageMs(1);
		 * if (sm != null) { i++; log.debug(i + " MESSAGE " + sm.toString()); }
		 * else { try { Thread.sleep(15000); } catch (InterruptedException e) {
		 * e.printStackTrace(); } } }
		 */
		// Init Delay ~30 sec for registration
		try {
			// TODO increase with more consumers
			Thread.sleep(35 * ADR_EM_Common.ONE_SECOND);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// TODO
		while (keepGoing) {
			TreeMap<String, InstructionsMessageContent> instrMap = elaborateInstructions(new ArrayList<UpdateMessageContent>(
					consumersUpdates.values()));
			sendInstructions(instrMap);
			
			try {
				// repeating the algorithm
				Thread.sleep(3 * ADR_EM_Common.ONE_MIN);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		

		this.closeConnection();
		log.debug("End of Aggregator");
	}

	private void sendInstructions(
			TreeMap<String, InstructionsMessageContent> instrMap) {
		ArrayList<InstructionsMessageContent> imcList = new ArrayList<InstructionsMessageContent>(
				instrMap.values());
		// TODO sends to every one even if there are not updates
		for (InstructionsMessageContent imc : imcList) {
			this.sendMessage(SimulationMessageFactory.getInstructionMessage(
					this.inputQueueName, imc.getConsumerReceiver(), imc));
		}
	}

	private TreeMap<String, InstructionsMessageContent> elaborateInstructions(
			ArrayList<UpdateMessageContent> _collectionUpdate) {

		List<UpdateMessageContent> collectionUpdate = _collectionUpdate;
		// Sort by time cut Desc
		Collections.sort(collectionUpdate,
				UpdateMessageContent.DescSortByTimeCutComparator);
		TreeMap<String, InstructionsMessageContent> outMsg = new TreeMap<String, InstructionsMessageContent>();

		// TODO TODO DELETE QUICK DEBUG
		// for (UpdateMessageContent updateMessageContent : collectionUpdate) {
		// log.info("ORDERED: " + updateMessageContent.toString());
		// }
		for (UpdateMessageContent updateMessageContent : collectionUpdate) {
			if (updateMessageContent.getTimeCut() != 0d)
				log.info(updateMessageContent.getConsumerSender()
						+ " - ORD_BY_TIME_CUT: "
						+ updateMessageContent.getTimeCut());
		}

		double targetFlexToCut = targetFlex;
		double freqActionBand = MAX_FCRN_FREQ_VARIATION - freqDeadBand;
		double band = NOMINAL_FREQ - freqDeadBand;
		double consumerPossibleCut;

		for (UpdateMessageContent updateMessageContent : collectionUpdate) {
			// if the rest of the consumers cannot cut else cond in the loop
			if (updateMessageContent.getTimeCut().compareTo(new Double(0d)) != 0
					&& updateMessageContent.getPossibleCut().compareTo(
							new Double(0d)) != 0 && targetFlexToCut > 0d) {
				// if we still have to reach the bottom_freq
				// TODO review the condition
				if (band > BOTTOM_FREQ) {
					consumerPossibleCut = updateMessageContent.getPossibleCut();
					// take down the possible cut
					targetFlexToCut -= consumerPossibleCut;
					// ration cut / target flex
					double ratio = consumerPossibleCut / targetFlex;
					double subFrequency = freqActionBand * ratio;
					band -= subFrequency;
					if (band <= BOTTOM_FREQ)
						band = BOTTOM_FREQ;
					// notify this consumer the
				}
				// the other consumers will be used for FCR-D
				else {
					band = BOTTOM_FREQ;
				}

				InstructionsMessageContent imc = new InstructionsMessageContent(
						updateMessageContent.getPossibleCut(), band, 0d, 0d,
						updateMessageContent.getConsumerSender());

				// Instruction
				outMsg.put(updateMessageContent.getConsumerSender(), imc);
			} else {
				// empty Instruction
				outMsg.put(
						updateMessageContent.getConsumerSender(),
						new InstructionsMessageContent(updateMessageContent
								.getConsumerSender()));
			}
		}

		// UPBUOND ALGORITHM
		log.info("Upperbuond Algorithm");
		// sort bu time increase
		Collections.sort(collectionUpdate,
				UpdateMessageContent.DescSortByTimeIncreaseComparator);
		// TODO TODO DELETE QUICK DEBUG
		for (UpdateMessageContent updateMessageContent : collectionUpdate) {
			if (updateMessageContent.getTimeIncrease() != 0d)
				log.info(updateMessageContent.getConsumerSender()
						+ " - Time increse ordered: "
						+ updateMessageContent.getTimeIncrease());
		}

		double targetFlexToIncrease = targetFlex;
		freqActionBand = MAX_FCRN_FREQ_VARIATION - freqDeadBand;
		band = NOMINAL_FREQ + freqDeadBand;
		double consumerPossibleIncrease;

		for (UpdateMessageContent updateMessageContent : collectionUpdate) {
			// if the rest of the consumers cannot cut else cond in the loop
			if (updateMessageContent.getTimeIncrease()
					.compareTo(new Double(0d)) != 0
					&& updateMessageContent.getPossibleIncrease().compareTo(
							new Double(0d)) != 0 && targetFlexToIncrease > 0d) {
				// if we still have to reach the bottom_freq
				// TODO review the condition
				if (band < TOP_FREQ) {
					consumerPossibleIncrease = updateMessageContent
							.getPossibleIncrease();
					// take down the possible cut
					targetFlexToIncrease -= consumerPossibleIncrease;
					// ration cut / target flex
					double ratio = consumerPossibleIncrease / targetFlex;
					double subFrequency = freqActionBand * ratio;
					band += subFrequency;
					if (band >= TOP_FREQ)
						band = TOP_FREQ;
					// notify this consumer the
				}
				// the other consumers will be used for FCR-D
				else {
					band = TOP_FREQ;
				}

				InstructionsMessageContent imc = outMsg
						.get(updateMessageContent.getConsumerSender());
				imc.setAboveNominalFrequency(band);
				imc.setAboveNominalIncrease(updateMessageContent
						.getPossibleIncrease());
				// Instruction
				outMsg.put(updateMessageContent.getConsumerSender(), imc);
			} else {
				// empty Instruction, taking into account the previous part of
				// the algorithm
				InstructionsMessageContent imc = outMsg
						.get(updateMessageContent.getConsumerSender());
				imc.setAboveNominalFrequency(0d);
				imc.setAboveNominalIncrease(0d);
				outMsg.put(updateMessageContent.getConsumerSender(), imc);
			}
		}

		// TODO quick debug TODO delete
		ArrayList<InstructionsMessageContent> imc = new ArrayList<InstructionsMessageContent>(
				outMsg.values());
		// for (InstructionsMessageContent instructionsMessageContent : imc) {
		// System.out.println(instructionsMessageContent.toString());
		// }
		log.info("FINAL RESULT Target = " + targetFlex);
		log.info("UNDERFREQ_RESULTS");
		Collections.sort(imc,
				InstructionsMessageContent.DescSortByUnderFrequency);
		double count = 0d;
		for (InstructionsMessageContent instructionsMessageContent : imc) {
			if (instructionsMessageContent.getUnderNominalFrequency() != 0)
				log.info(instructionsMessageContent.getConsumerReceiver()
						+ " - "
						+ instructionsMessageContent.getUnderNominalFrequency()
						+ " - "
						+ instructionsMessageContent.getUnderNominalDecrease());
			count += instructionsMessageContent.getUnderNominalDecrease();
		}
		log.info("TOTAL FLEX UNDER : " + count);
		count = 0d;
		log.info("ABOVE FREQ RESULTS");
		Collections.sort(imc,
				InstructionsMessageContent.AscSortByAboveFrequency);

		for (InstructionsMessageContent instructionsMessageContent : imc) {
			if (instructionsMessageContent.getAboveNominalFrequency() != 0)
				log.info(instructionsMessageContent.getConsumerReceiver()
						+ " - "
						+ instructionsMessageContent.getAboveNominalFrequency()
						+ " - "
						+ instructionsMessageContent.getAboveNominalIncrease());
			count += instructionsMessageContent.getAboveNominalIncrease();
		}
		log.info("TOTAL FLEX Above : " + count);

		return outMsg;

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

	public static double getFreqband() {
		return freqDeadBand;
	}

	public static double getTargetFlex() {
		return targetFlex;
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
			log.debug("Message SENt WITHOUT UPDATE CONTENT");
		}
	}

}