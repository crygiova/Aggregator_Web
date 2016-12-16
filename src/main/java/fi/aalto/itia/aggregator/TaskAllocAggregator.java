package fi.aalto.itia.aggregator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import fi.aalto.itia.adr_em_common.ADR_EM_Common;
import fi.aalto.itia.adr_em_common.InstructionsMessageContent;
import fi.aalto.itia.adr_em_common.SimulationElement;
import fi.aalto.itia.adr_em_common.SimulationMessage;
import fi.aalto.itia.adr_em_common.SimulationMessageFactory;
import fi.aalto.itia.adr_em_common.StatsToAggUpdateContent;
import fi.aalto.itia.adr_em_common.UpdateMessageContent;
import fi.aalto.itia.util.MaxSizedArray;
import fi.aalto.itia.util.Utility;

/**
 * @author giovanc1
 *
 */
public class TaskAllocAggregator extends SimulationElement {

    /**
	 * 
	 */
    private static final long serialVersionUID = -5676710132282560560L;
    public static final String FILE_NAME_PROPERTIES = "agg_config.properties";
    private static final String TARGET_FLEX = "TARGET_FLEX";
    private static final String FREQ_BAND = "FREQ_BAND";
    private static final String BASE_NOMINAL = "BASE_NOMINAL";
    private static final String CONSTANT_BASE_NOMINAL = "CONSTANT_BASE_NOMINAL";
    private static final String ERRORS = "ERRORS";
    private static final String PER_ERROR = "PER_ERROR";
    private static final String BASE_NOMINAL_ERROR = "BASE_NOMINAL_ERROR";
    private static final double MAX_FCRN_FREQ_VARIATION = 0.1d;
    private static final double NOMINAL_FREQ = 50d;
    private static final double BOTTOM_FREQ = NOMINAL_FREQ - MAX_FCRN_FREQ_VARIATION;
    private static final double TOP_FREQ = NOMINAL_FREQ + MAX_FCRN_FREQ_VARIATION;
    // time costant for the delay of the deadband control in seconds
    private static final int TAU = 15;
    // for the run method says the max number of loops for an update(e.g. if
    // aggregator checks every 30 sec, every 5 min (10) there must be an update)

    private static final int MAX_NUM_LOOP = 15 * ADR_EM_Common.ONE_MIN_IN_SEC / TAU;
    // minutes
    private static final int INITIAL_DELAY_SEC = 2 * 60;
    // factor of consumer that sent an update that were involved in the DR
    private static final double UPDATE_AFTER_PERCENT = 0.4d;

    private static Logger log = Logger.getLogger("aggregator.log");
    private static TaskAllocAggregator agg;
    private static final double targetFlex;
    private static double baseNominal;
    private static final double baseNominalError;
    private static final double freqDeadBand;

    private boolean enableDeadControl = true;

    // ERRORS INJECTIOn
    private static boolean injectErrors = false;
    private static final double PERCENT_ERROR;

    // TODO finish to use these 2 param properly
    private int newUpdates = 0;
    private int numberOfConsumers = 0;

    // calculated consumption without ADR
    private double aggregatedNoADRConsumption = baseNominal;
    // base nominal updated by taking the mean ove the last n elements
    private final static int MEAN_BASE_NOMINAL = 75;
    // real theoretical consumption
    private final static MaxSizedArray theoreticalConsumption = new MaxSizedArray(MEAN_BASE_NOMINAL);
    // the threashold distance between the baseNOminal and the real
    // aggregatedNoADRConsumption 1Kw
    private static final double THRESHOLD_DEAD_CONTROL = 250;
    // TODO XXX DELETE THIS ONCE THE SMALL BUG WITH THE DEAD BAND CONTROL IS
    // SOLVEd
    //private static final double FRIDGE_CONSUMPTION = 100d;

    private static final boolean useConstantBaseNominal;

    // Keeps track of registered consumers
    private Set<String> consumers;

    // sets used for the elaborateInstruction algorithm to take into account who
    // and how many got the instructions in the last iteration

    private Map<String, ConsumerFlexInfo> adrReactionOverFreq;
    private Map<String, ConsumerFlexInfo> adrReservesOverFreq;
    // they should react for balancing to the nominal
    private Map<String, ConsumerFlexInfo> adrDeadOverFreq;

    private Map<String, ConsumerFlexInfo> adrReactionUnderFreq;
    private Map<String, ConsumerFlexInfo> adrReservesUnderFreq;
    // TODO they should react for balancing to the nominal
    private Map<String, ConsumerFlexInfo> adrDeadUnderFreq;

    // counters for how many has responded already to the
    private int counterOverFreq = 0;
    private int counterUnderFreq = 0;

    // out of it and order it with Comparators
    private TreeMap<String, UpdateMessageContent> consumersUpdates;

    //
    private boolean firstAllocation = true;

    static {
	Properties properties = Utility.getProperties(FILE_NAME_PROPERTIES);
	//targetFlex = Double.parseDouble(properties.getProperty(TARGET_FLEX));
	targetFlex = ADR_EM_Common.TARGET_FLEX;
	baseNominal = Double.parseDouble(properties.getProperty(BASE_NOMINAL));
	PERCENT_ERROR = Double.parseDouble(properties.getProperty(PER_ERROR));
	// Percentage nominal error
	baseNominalError = Double.parseDouble(properties.getProperty(BASE_NOMINAL_ERROR))
		* targetFlex;
	freqDeadBand = Double.parseDouble(properties.getProperty(FREQ_BAND));
	injectErrors = Boolean.parseBoolean(properties.getProperty(ERRORS));
	// constant base nominal
	useConstantBaseNominal = Boolean
		.parseBoolean(properties.getProperty(CONSTANT_BASE_NOMINAL));
	// File Handler
	FileHandler fh;
	try {
	    // This block configure the logger with handler and formatter
	    fh = new FileHandler(ADR_EM_Common.OUT_FILE_DIR + "aggregator.log");
	    log.addHandler(fh);
	    SimpleFormatter formatter = new SimpleFormatter();
	    fh.setFormatter(formatter);
	    // the following statement is used to log any messages
	} catch (SecurityException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    private TaskAllocAggregator(String inputQueueName) {
	super(inputQueueName);
	consumers = new LinkedHashSet<String>();
	initConsumersHashMaps();
	consumersUpdates = new TreeMap<String, UpdateMessageContent>();
    }

    // Singleton implementation,
    public static TaskAllocAggregator getInstance() {
	return agg;
    }

    // New instance of aggregator
    public static TaskAllocAggregator getNewInstance(String inputQueue) {
	agg = new TaskAllocAggregator(inputQueue);
	return agg;
    }

    @Override
    public void run() {
	// init as max so it execute immediately the instructions algorithm
	int countLoops = MAX_NUM_LOOP;

	this.startConsumingMq();
	log.info("Start Aggregator");
	/*
	 * while (keepGoing) { /*SimulationMessage sm = this.pollMessageMs(1);
	 * if (sm != null) { i++; log.debug(i + " MESSAGE " + sm.toString()); }
	 * else { try { Thread.sleep(15000); } catch (InterruptedException e) {
	 * e.printStackTrace(); } } }
	 */
	// Init Delay ~30 sec for registration
	try {

	    // 5000 circa 2 min of time
	    Thread.sleep(INITIAL_DELAY_SEC * ADR_EM_Common.ONE_SECOND);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	while (keepGoing) {
	    // V1
	    // TreeMap<String, InstructionsMessageContent> instrMap =
	    // elaborateInstructions(new ArrayList<UpdateMessageContent>(
	    // consumersUpdates.values()));

	    if (countLoops == MAX_NUM_LOOP) {// update
		TreeMap<String, InstructionsMessageContent> instrMap = elaborateInstructionsV4(new ArrayList<UpdateMessageContent>(
			consumersUpdates.values()));
		if (countLoops == MAX_NUM_LOOP) {
		    log.info("COUNT LOOP MAX ");
		} else {
		    log.info("COUNTERs TRIGGERED THE UPDATE");
		}
		// if errors are to be injected
		if (injectErrors) {
		    sendInstructionsWithErrors(instrMap);
		} else {
		    sendInstructions(instrMap);
		    if (firstAllocation)
			firstAllocation = false;
		}

		countLoops = 0;
	    }
	    // deadband control
	    if (!firstAllocation && enableDeadControl) {
		this.deadBandControl();
	    }

	    try {
		// repeating the algorithm 30 sec
		Thread.sleep(TAU * ADR_EM_Common.ONE_SECOND);
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	    // loop counter
	    countLoops++;
	}

	this.closeConnection();
	log.info("End of Aggregator");
    }

    private synchronized void sendInstructions(TreeMap<String, InstructionsMessageContent> instrMap) {
	ArrayList<InstructionsMessageContent> imcList = new ArrayList<InstructionsMessageContent>(
		instrMap.values());
	// send to the stats an empty simulation message that says it is a
	// new update for the consumers
	this.sendMessage(SimulationMessageFactory.getEmptyAggToStatsMessage(this.inputQueueName,
		ADR_EM_Common.STATS_NAME_QUEUE), false);
	// sends to every one even if there are not updates
	for (InstructionsMessageContent imc : imcList) {
	    this.sendMessage(SimulationMessageFactory.getInstructionMessage(this.inputQueueName,
		    imc.getConsumerReceiver(), imc));

	}
    }

    private void sendInstructionsWithErrors(TreeMap<String, InstructionsMessageContent> instrMap) {
	ArrayList<InstructionsMessageContent> imcList = new ArrayList<InstructionsMessageContent>(
		instrMap.values());
	// send to the stats an empty simulation message that says it is a
	// new update for the consumers
	// used in case of error
	Random rand = new Random();
	boolean sendMsg;
	this.sendMessage(SimulationMessageFactory.getEmptyAggToStatsMessage(this.inputQueueName,
		ADR_EM_Common.STATS_NAME_QUEUE));
	// sends with some errors based on PERCENT_ERROR
	for (InstructionsMessageContent imc : imcList) {
	    sendMsg = rand.nextDouble() > PERCENT_ERROR ? true : false;
	    if (sendMsg) {
		this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			this.inputQueueName, imc.getConsumerReceiver(), imc));
	    }
	}
    }

    // XXX Fourth Version version of EleborateInstructions
    public synchronized TreeMap<String, InstructionsMessageContent> elaborateInstructionsV4(
	    ArrayList<UpdateMessageContent> _collectionUpdate) {
	log.info("**********Elaborate Instructions********************");
	List<UpdateMessageContent> collectionUpdate = _collectionUpdate;
	// Sort by time cut Desc
	// shffle
	Collections.shuffle(collectionUpdate);
	Collections.sort(collectionUpdate, UpdateMessageContent.OrderForDwElabComparator);
	TreeMap<String, InstructionsMessageContent> outMsg = new TreeMap<String, InstructionsMessageContent>();

	// Init HASH MAPS
	initADRHashMaps();
	// flexibility to cut
	double targetFlexToCut = targetFlex;
	// active ADR frequency
	double freqActionBand = MAX_FCRN_FREQ_VARIATION - freqDeadBand;
	// used to stabilize baseNominal with Aggregated no ADR consumption
	double flexControl = 0;// baseNominal - aggregatedNoADRConsumption;
	// starting band
	double band = NOMINAL_FREQ - freqDeadBand;
	double bufferBand = band;
	double consumerPossibleCut;

	for (UpdateMessageContent updateMessageContent : collectionUpdate) {
	    // if the rest of the consumers cannot cut else cond in the loop
	    if (updateMessageContent.getTimeCut().compareTo(new Double(0d)) != 0
		    && updateMessageContent.getPossibleCut().compareTo(new Double(0d)) != 0
		    && targetFlexToCut > 0d) {
		// if we still have to reach the bottom_freq
		// review the condition
		consumerPossibleCut = updateMessageContent.getPossibleCut();
		// deadband control
		if (band > BOTTOM_FREQ) {
		    // take down the possible cut
		    targetFlexToCut -= consumerPossibleCut;
		    // ration cut / target flex
		    double ratio = consumerPossibleCut / targetFlex;
		    double subFrequency = freqActionBand * ratio;
		    band -= subFrequency;
		    if (band <= BOTTOM_FREQ)
			band = BOTTOM_FREQ;
		    // notify this consumer the
		    // add toSet of instructions for above
		    bufferBand = band;
		    adrReactionUnderFreq.put(updateMessageContent.getConsumerSender(),
			    new ConsumerFlexInfo(updateMessageContent.getConsumerSender(),
				    consumerPossibleCut, bufferBand));

		}
		// the other consumers will be used for FCR-D
		else {

		    bufferBand = 0d;
		}
		InstructionsMessageContent imc = new InstructionsMessageContent(
			updateMessageContent.getPossibleCut(), bufferBand, 0d, 0d,
			updateMessageContent.getConsumerSender());

		// Instruction
		outMsg.put(updateMessageContent.getConsumerSender(), imc);

	    } else {
		// empty Instruction
		if (updateMessageContent.getPossibleCut() > 0) {
		    // FILL The REserves
		    adrReservesUnderFreq.put(updateMessageContent.getConsumerSender(),
			    new ConsumerFlexInfo(updateMessageContent.getConsumerSender(),
				    updateMessageContent.getPossibleCut(), 0));
		}

		outMsg.put(updateMessageContent.getConsumerSender(),
			new InstructionsMessageContent(updateMessageContent.getConsumerSender()));
	    }
	}

	// UPBUOND ALGORITHM
	// log.info("\n-------UPBUOND ALGORITHM-------");
	// sort bu time increase
	Collections.sort(collectionUpdate, UpdateMessageContent.OrderForUpElabComparator);

	double targetFlexToIncrease = targetFlex;
	flexControl = 0;// baseNominal - aggregatedNoADRConsumption;
	freqActionBand = MAX_FCRN_FREQ_VARIATION - freqDeadBand;
	band = NOMINAL_FREQ + freqDeadBand;
	double consumerPossibleIncrease;

	for (UpdateMessageContent updateMessageContent : collectionUpdate) {
	    // if the rest of the consumers cannot cut else cond in the loop
	    if (updateMessageContent.getTimeIncrease().compareTo(new Double(0d)) != 0
		    && updateMessageContent.getPossibleIncrease().compareTo(new Double(0d)) != 0
		    && targetFlexToIncrease > 0d) {
		// if we still have to reach the bottom_freq
		// review the condition
		consumerPossibleIncrease = updateMessageContent.getPossibleIncrease();
		if (band < TOP_FREQ) {
		    consumerPossibleIncrease = updateMessageContent.getPossibleIncrease();
		    // take down the possible cut
		    targetFlexToIncrease -= consumerPossibleIncrease;
		    // ration cut / target flex
		    double ratio = consumerPossibleIncrease / targetFlex;
		    double subFrequency = freqActionBand * ratio;
		    band += subFrequency;
		    if (band >= TOP_FREQ)
			band = TOP_FREQ;
		    // notify this consumer the
		    // add toSet of instructions for above
		    bufferBand = band;
		    adrReactionOverFreq.put(updateMessageContent.getConsumerSender(),
			    new ConsumerFlexInfo(updateMessageContent.getConsumerSender(),
				    updateMessageContent.getPossibleIncrease(), bufferBand));
		} else {
		    bufferBand = 0d;
		}

		InstructionsMessageContent imc = outMsg.get(updateMessageContent
			.getConsumerSender());
		imc.setAboveNominalFrequency(bufferBand);
		imc.setAboveNominalIncrease(updateMessageContent.getPossibleIncrease());
		// Instruction
		outMsg.put(updateMessageContent.getConsumerSender(), imc);

	    } else {
		// empty Instruction, taking into account the previous part of
		// the algorithm
		// FILL The reserves
		if (updateMessageContent.getPossibleIncrease() > 0) {
		    adrReservesOverFreq.put(updateMessageContent.getConsumerSender(),
			    new ConsumerFlexInfo(updateMessageContent.getConsumerSender(),
				    updateMessageContent.getPossibleIncrease(), bufferBand));
		}
		InstructionsMessageContent imc = outMsg.get(updateMessageContent
			.getConsumerSender());
		imc.setAboveNominalFrequency(0d);
		imc.setAboveNominalIncrease(0d);
		outMsg.put(updateMessageContent.getConsumerSender(), imc);
	    }
	}

	// quick debug delete
	ArrayList<InstructionsMessageContent> imc = new ArrayList<InstructionsMessageContent>(
		outMsg.values());
	// for (InstructionsMessageContent instructionsMessageContent : imc) {
	// System.out.println(instructionsMessageContent.toString());
	// }
	log.info("+++++++++++++++++++ TargetFlex = " + targetFlex);
	log.info("\nUnder Freq:");
	Collections.sort(imc, InstructionsMessageContent.DescSortByUnderFrequency);
	double count = 0d;
	for (InstructionsMessageContent instructionsMessageContent : imc) {
	    if (instructionsMessageContent.getUnderNominalFrequency() != 0)
		// log.info(instructionsMessageContent.getConsumerReceiver() +
		// " - "
		// + instructionsMessageContent.getUnderNominalFrequency() +
		// " - "
		// + instructionsMessageContent.getUnderNominalDecrease());
		count += instructionsMessageContent.getUnderNominalDecrease();
	}
	log.info("TOTAL FLEX UNDER : " + count);
	log.info("ALLOC:" + adrReactionUnderFreq.size() + "  RESERVES"
		+ adrReservesUnderFreq.size());
	count = 0d;
	log.info("\nAbove Freq");
	Collections.sort(imc, InstructionsMessageContent.AscSortByAboveFrequency);

	for (InstructionsMessageContent instructionsMessageContent : imc) {
	    if (instructionsMessageContent.getAboveNominalFrequency() != 0)
		// log.info(instructionsMessageContent.getConsumerReceiver() +
		// " - "
		// + instructionsMessageContent.getAboveNominalFrequency() +
		// " - "
		// + instructionsMessageContent.getAboveNominalIncrease());
		count += instructionsMessageContent.getAboveNominalIncrease();
	}
	log.info("TOTAL FLEX Above : " + count);
	log.info("ALLOC:" + adrReactionOverFreq.size() + "  RESERVES" + adrReservesOverFreq.size());
	log.info("\n**********End Algorithm********************");

	// SET Base nominal if constant
	if (useConstantBaseNominal) {
	    baseNominal = theoreticalConsumption.getLast();
	    deadBandControl();
	}
	return outMsg;
    }

    // change this one when there is a message coming in for each actor
    public void startConsumingMq() {
	Consumer consumer = new DefaultConsumer(dRChannel) {
	    @Override
	    public void handleDelivery(String consumerTag, Envelope envelope,
		    AMQP.BasicProperties properties, byte[] body) throws IOException {
		SimulationMessage sm = null;
		try {
		    sm = (SimulationMessage) SimulationMessage.deserialize(body);
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
	// log.info(sm.toString());
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
	case ADR_EM_Common.STATS_TO_AGG_HEADER:
	    // getting a StatsToAggUpdateContent with current nominal
	    // consumption
	    aggregatedNoADRConsumption = ((StatsToAggUpdateContent) sm.getContent())
		    .getCurrentNominalAggregatedConsumption();
	    theoreticalConsumption.addElement(aggregatedNoADRConsumption);
	    // update base nominal
	    if (!useConstantBaseNominal) {
		baseNominal = theoreticalConsumption.getMean();
	    }
	    //NoDElay Message
	    this.sendMessage(SimulationMessageFactory.getStatsToAggUpdateMessage(
		    this.inputQueueName, sm.getSender(), new StatsToAggUpdateContent(baseNominal)), false);
	    // printout
	    break;
	default:
	    addMessage(sm);
	    break;
	}
    }

    private synchronized void deadBandControl() {
	double deltaControl = Math.abs(aggregatedNoADRConsumption - baseNominal);
	// 1KwDistance
	if (aggregatedNoADRConsumption < baseNominal - THRESHOLD_DEAD_CONTROL) {
	    log.info("aggregatedNoADRConsumption < baseNomial");
	    // baseNominal > aggregatedNoADRConsumption -> need to consume
	    if (!adrDeadUnderFreq.isEmpty()) {
		// log.info("!adrDeadUnderFreq.isEmpty()");
		// if not empty need to send a stop messageto all of the
		// consumers
		for (Map.Entry<String, ConsumerFlexInfo> entry : adrDeadUnderFreq.entrySet()) {
		    // log.info("Loop 1");
		    ConsumerFlexInfo reserve = entry.getValue();
		    String reserveName = reserve.getName();
		    // deallocate
		    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d, 0d, 0d,
			    reserveName);
		    // send MSg to substitute
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, reserveName, imc));
		}
		// empty the set
		adrDeadUnderFreq.clear();
	    }
	    double currentDeadControl = countCurrentDeadControl(adrDeadOverFreq);
	    double newDeadControl = Math.abs(currentDeadControl - deltaControl);
	    // log.info("DISTANCE TO BE COVERED NewDeadControl" +
	    // newDeadControl);
	    if (newDeadControl > 0d) {
		if (currentDeadControl > deltaControl) {
		    // log.info("currentDeadControl > deltaControl");
		    // need less control
		    // send empty message to some elements in
		    // adrNominalAboveFreq
		    while (!adrDeadOverFreq.isEmpty() && newDeadControl > 0) {
			log.info("Loop 2");
			if (adrDeadOverFreq.entrySet().iterator().hasNext()) {
			    log.info("Loop 2 Remove");
			    ConsumerFlexInfo reserve = adrDeadOverFreq.entrySet().iterator().next()
				    .getValue();
			    String reserveName = reserve.getName();
			    double flex = reserve.getFlexibility();
			    // deallocate
			    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d,
				    0d, 0d, reserveName);
			    // send MSg to substitute
			    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
				    this.inputQueueName, reserveName, imc));
			    adrDeadOverFreq.remove(reserveName);
			    newDeadControl -= flex;//FRIDGE_CONSUMPTION;
			} else {
			    break;
			}
		    }
		} else if (currentDeadControl < deltaControl) {
		    // log.info("currentDeadControl < deltaControl");
		    // need more control
		    // allocate new element in adrNominalAboveFreq
		    double freq = NOMINAL_FREQ - freqDeadBand - MAX_FCRN_FREQ_VARIATION;
		    while (!adrReservesOverFreq.isEmpty() && newDeadControl > 0) {
			// log.info("Loop 3");
			if (adrReservesOverFreq.entrySet().iterator().hasNext()) {
			    ConsumerFlexInfo reserve = adrReservesOverFreq.entrySet().iterator()
				    .next().getValue();
			    double flex = reserve.getFlexibility();
			    String reserveName = reserve.getName();
			    reserve.setFrequencyAllocated(freq);
			    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d,
				    flex, freq, reserveName);
			    // send MSg to substitute
			    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
				    this.inputQueueName, reserveName, imc));
			    adrReservesOverFreq.remove(reserveName);
			    adrDeadOverFreq.put(reserveName, reserve);
			    newDeadControl -= flex;//FRIDGE_CONSUMPTION;// TODO XXX
								 // CHANGE
			    if (consumersUpdates.containsKey(reserveName)) {
				// remove from the ADR control
				consumersUpdates.remove(reserveName);
			    }
			} else {
			    break;
			}
		    }
		}
	    }
	} else if (aggregatedNoADRConsumption > baseNominal + THRESHOLD_DEAD_CONTROL) {
	    // baseNominal < aggregatedNoADRConsumption -> need to save
	    log.info("aggregatedNoADRConsumption > baseNomial ");
	    if (!adrDeadOverFreq.isEmpty()) {
		// if not empty need to send a stop message to all of the
		// consumers
		for (Map.Entry<String, ConsumerFlexInfo> entry : adrDeadOverFreq.entrySet()) {
		    // log.info("Loop 4");
		    ConsumerFlexInfo reserve = entry.getValue();
		    String reserveName = reserve.getName();
		    double flex = reserve.getFlexibility();
		    // deallocate
		    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d, 0d, 0d,
			    reserveName);
		    // send empty Msg
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, reserveName, imc));
		}
		// empty the set
		adrDeadOverFreq.clear();
	    }

	    double currentDeadControl = countCurrentDeadControl(adrDeadUnderFreq);
	    double newDeadControl = Math.abs(currentDeadControl - deltaControl);
	    // log.info("(newDeadControl > 100)" + (newDeadControl >
	    // THRESHOLD_DEAD_CONTROL));
	    if (newDeadControl > 0d) {
		if (currentDeadControl > deltaControl) {
		    // log.info("currentDeadControl > deltaControl");
		    // need lees control
		    // send empty message to some elements in
		    // adrConsumersBelowFreq
		    while (!adrDeadUnderFreq.isEmpty() && newDeadControl > 0) {
			// log.info("Loop 5");
			if (adrDeadUnderFreq.entrySet().iterator().hasNext()) {
			    ConsumerFlexInfo reserve = adrDeadUnderFreq.entrySet().iterator()
				    .next().getValue();
			    String reserveName = reserve.getName();
			    double flex = reserve.getFlexibility();
			    // deallocate
			    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d,
				    0d, 0d, reserveName);
			    // send MSg to substitute
			    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
				    this.inputQueueName, reserveName, imc));
			    adrDeadUnderFreq.remove(reserveName);
			    newDeadControl -= flex;
			} else {
			    break;
			}
		    }
		}

		if (currentDeadControl < deltaControl) {
		    // log.info("currentDeadControl < deltaControl");
		    // need more control
		    // allocate new element in adrConsumersBelowFreq
		    double freq = NOMINAL_FREQ + freqDeadBand + MAX_FCRN_FREQ_VARIATION;
		    while (!adrReservesUnderFreq.isEmpty() && newDeadControl > 0) {
			if (adrReservesUnderFreq.entrySet().iterator().hasNext()) {
			    // log.info("Loop 6");
			    ConsumerFlexInfo reserve = adrReservesUnderFreq.entrySet().iterator()
				    .next().getValue();
			    double flex = reserve.getFlexibility();
			    String reserveName = reserve.getName();
			    reserve.setFrequencyAllocated(freq);
			    InstructionsMessageContent imc = new InstructionsMessageContent(flex,
				    freq, 0d, 0d, reserveName);
			    // send MSg to substitute
			    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
				    this.inputQueueName, reserveName, imc));
			    adrReservesUnderFreq.remove(reserveName);
			    adrDeadUnderFreq.put(reserveName, reserve);
			    newDeadControl -= flex;
			    if (consumersUpdates.containsKey(reserveName)) {
				// remove from the ADR control
				consumersUpdates.remove(reserveName);
			    }
			} else {
			    break;
			}
		    }
		}
	    }
	} else {
	}/*
	  * // Empty all for (Map.Entry<String, ConsumerFlexInfo> entry :
	  * adrDeadOverFreq.entrySet()) { ConsumerFlexInfo reserve =
	  * entry.getValue(); String reserveName = reserve.getName(); //
	  * deallocate InstructionsMessageContent imc = new
	  * InstructionsMessageContent(0d, 0d, 0d, 0d, reserveName); // send
	  * empty Msg
	  * this.sendMessage(SimulationMessageFactory.getInstructionMessage(
	  * this.inputQueueName, reserveName, imc)); }
	  * 
	  * for (Map.Entry<String, ConsumerFlexInfo> entry :
	  * adrDeadUnderFreq.entrySet()) { ConsumerFlexInfo reserve =
	  * entry.getValue(); String reserveName = reserve.getName(); //
	  * deallocate InstructionsMessageContent imc = new
	  * InstructionsMessageContent(0d, 0d, 0d, 0d, reserveName); // send MSg
	  * to substitute
	  * this.sendMessage(SimulationMessageFactory.getInstructionMessage(
	  * this.inputQueueName, reserveName, imc)); } }
	  */

	log.info("AGG_CONS_NO_ADR: " + aggregatedNoADRConsumption);
	log.info("BASE NOM: " + baseNominal);
	log.info("DIfference : " + (aggregatedNoADRConsumption - baseNominal));
	log.info("UNDER DEAD CONTROL: " + adrDeadUnderFreq.size());
	log.info("OVER DEAD CONTROL: " + adrDeadOverFreq.size());
    }

    private double countCurrentDeadControl(Map<String, ConsumerFlexInfo> setOfCOnsumers) {

	double currendDeadControl = 0d;
	for (Map.Entry<String, ConsumerFlexInfo> entry : setOfCOnsumers.entrySet()) {
	    // log.info("Loop 10");
	    ConsumerFlexInfo reserve = entry.getValue();
	    currendDeadControl += reserve.getFlexibility();// TODO
						     // reserve.getFlexibility();
	}
	return currendDeadControl;
    }

    // Consumer Registration to the DR system
    public void addConsumer(SimulationMessage registrationMsg) {
	if (!consumers.contains(registrationMsg.getSender())) {
	    consumers.add(registrationMsg.getSender());
	    // log.info("Consumer Registered");
	    this.sendMessage(SimulationMessageFactory.getRegisterAccept(inputQueueName,
		    registrationMsg.getSender()), false);
	    numberOfConsumers++;
	} else {
	    // log.info("Consumer ALREADY Registered");
	    this.sendMessage(SimulationMessageFactory.getRegisterDeny(inputQueueName,
		    registrationMsg.getSender()), false);
	}
    }

    private void initConsumersHashMaps() {
	initADRHashMaps();
	initDeadBandHashMaps();
	counterOverFreq = 0;
	counterUnderFreq = 0;
    }

    private void initDeadBandHashMaps() {
	adrDeadOverFreq = Collections.synchronizedMap(new HashMap<String, ConsumerFlexInfo>());
	adrDeadUnderFreq = Collections.synchronizedMap(new HashMap<String, ConsumerFlexInfo>());
    }

    private void initADRHashMaps() {
	adrReactionOverFreq = Collections.synchronizedMap(new HashMap<String, ConsumerFlexInfo>());
	adrReservesOverFreq = Collections.synchronizedMap(new HashMap<String, ConsumerFlexInfo>());
	adrReactionUnderFreq = Collections.synchronizedMap(new HashMap<String, ConsumerFlexInfo>());
	adrReservesUnderFreq = Collections.synchronizedMap(new HashMap<String, ConsumerFlexInfo>());
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

    public TreeMap<String, UpdateMessageContent> getConsumersUpdates() {
	return consumersUpdates;
    }

    public void setConsumersUpdates(TreeMap<String, UpdateMessageContent> consumersUpdates) {
	this.consumersUpdates = consumersUpdates;
    }

    public synchronized void addToConsumersUpdates(Serializable umc) {
	// if the content of the message is UpdateMessageContent
	if (umc instanceof UpdateMessageContent) {
	    UpdateMessageContent uMsg = (UpdateMessageContent) umc;
	    // Consumer Identifier
	    String consumerIoQueue = uMsg.getConsumerSender();
	    // says if the update needs to be added to the consumer updates
	    boolean addToConsumerUpdates = true;
	    // this can be improved (what if one consumer keeps sending
	    // updates??)
	    this.newUpdates++;
	    // log.info("UPDATE CONSUMER" + consumerIoQueue);
	    if (adrReactionOverFreq.containsKey(consumerIoQueue)) {
		// if C cannot provide anymore flex
		if (uMsg.getPossibleIncrease() == 0 && !adrReservesOverFreq.isEmpty()
			&& adrReservesOverFreq.entrySet().iterator().hasNext()) {
		    double freq = adrReactionOverFreq.get(consumerIoQueue).getFrequencyAllocated();
		    double flex = adrReactionOverFreq.get(consumerIoQueue).getFlexibility();
		    // find a random substitute
		    Random generator = new Random();
		    Object[] values = adrReservesOverFreq.values().toArray();
		    ConsumerFlexInfo reserve = (ConsumerFlexInfo) values[generator
			    .nextInt(values.length)];
		    // ConsumerFlexInfo reserve =
		    // adrReservesOverFreq.entrySet().iterator().next()
		    // .getValue();
		    reserve.setFrequencyAllocated(freq);
		    String reserveName = reserve.getName();
		    // send MSg to substitute
		    // out to the one that exits
		    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d, 0d, 0d,
			    consumerIoQueue);
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, consumerIoQueue, imc));
		    // TODO XXX Send the new Update
		    imc = new InstructionsMessageContent(0d, 0d, flex,
			    freq, reserveName);
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, reserveName, imc));
		    // remove it
		    adrReactionOverFreq.remove(consumerIoQueue);
		    adrReservesOverFreq.remove(reserve.getName());
		    adrReactionOverFreq.put(reserve.getName(), reserve);
		}
		// counting the consumer that has sent a new update and he was
		// requested to perform ADR in the last instruction update
		counterOverFreq++;
	    } else if (adrReactionUnderFreq.containsKey(consumerIoQueue)) {
		// if C cannot cut anymore
		if (uMsg.getPossibleCut() == 0 && !adrReactionUnderFreq.isEmpty()
			&& adrReservesUnderFreq.entrySet().iterator().hasNext()) {
		    double freq = adrReactionUnderFreq.get(consumerIoQueue).getFrequencyAllocated();
		    double flex = adrReactionUnderFreq.get(consumerIoQueue).getFlexibility();
		    // find a randoom substitute take first element
		    Random generator = new Random();
		    Object[] values = adrReservesUnderFreq.values().toArray();
		    ConsumerFlexInfo reserve = (ConsumerFlexInfo) values[generator
			    .nextInt(values.length)];
		    // ConsumerFlexInfo reserve =
		    // adrReservesUnderFreq.entrySet().iterator().next()
		    // .getValue();
		    reserve.setFrequencyAllocated(freq);
		    String reserveName = reserve.getName();
		    // out to the one that exits
		    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d, 0d, 0d,
			    consumerIoQueue);
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, consumerIoQueue, imc));
		    // send MSg to substitute
		    imc = new InstructionsMessageContent(flex, freq, 0d,
			    0d, reserveName);
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, reserveName, imc));
		    // remove prevous
		    adrReactionUnderFreq.remove(consumerIoQueue);
		    // add substitute to BelowFreq
		    adrReservesUnderFreq.remove(reserveName);
		    adrReactionUnderFreq.put(reserveName, reserve);
		}
		// counting the consumer that has sent a new update and he was
		// requested to perform ADR in the last instruction update
		counterUnderFreq++;
	    } else if (adrReservesOverFreq.containsKey(consumerIoQueue)) {
		// just remove if it has no more flex to offer
		if (uMsg.getPossibleIncrease() == 0) {
		    adrReservesOverFreq.remove(consumerIoQueue);
		}
	    } else if (adrReservesUnderFreq.containsKey(consumerIoQueue)) {
		// just remove if it has no moreflex to offer
		if (uMsg.getPossibleCut() == 0) {
		    adrReservesUnderFreq.remove(consumerIoQueue);
		}
	    } else if (adrDeadOverFreq.containsKey(consumerIoQueue)) {
		// only remove elaborate shift will replace it
		if (uMsg.getPossibleIncrease() == 0) {
		    adrDeadOverFreq.remove(consumerIoQueue);
		    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d, 0d, 0d,
			    consumerIoQueue);
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, consumerIoQueue, imc));
		} else {
		    // do not consider in the next allocation the consumers used
		    // in the deadband control
		    addToConsumerUpdates = false;
		    if (consumersUpdates.containsKey(consumerIoQueue)) {
			// remove from the ADR control
			consumersUpdates.remove(consumerIoQueue);
		    }
		}
	    } else if (adrDeadUnderFreq.containsKey(consumerIoQueue)) {
		// only remove elaborate shift will replace it

		if (uMsg.getPossibleCut() == 0) {
		    adrDeadUnderFreq.remove(consumerIoQueue);
		    InstructionsMessageContent imc = new InstructionsMessageContent(0d, 0d, 0d, 0d,
			    consumerIoQueue);
		    this.sendMessage(SimulationMessageFactory.getInstructionMessage(
			    this.inputQueueName, consumerIoQueue, imc));
		} else {
		    // do not consider in the next allocation the consumers used
		    // in the deadband control
		    addToConsumerUpdates = false;
		    // remove it from consumerUpdates
		    if (consumersUpdates.containsKey(consumerIoQueue)) {
			// remove from the ADR control
			consumersUpdates.remove(consumerIoQueue);
		    }
		}
	    }
	    // Overwrite with the last update
	    if (addToConsumerUpdates) {
		consumersUpdates.put(consumerIoQueue, uMsg);
	    }
	} else {
	    log.info("Message SENt WITHOUT UPDATE CONTENT");
	}
    }

    // This inner class describes the essentials elements of an allocated
    // consumer for FC
    class ConsumerFlexInfo {
	private String name;
	private double flexibility;
	private double frequencyAllocated;

	public ConsumerFlexInfo(String name, double flexibility, double frequencyAllocated) {
	    this.name = name;
	    this.flexibility = flexibility;
	    this.frequencyAllocated = frequencyAllocated;
	}

	public String getName() {
	    return name;
	}

	public void setName(String name) {
	    this.name = name;
	}

	public double getFlexibility() {
	    return flexibility;
	}

	public void setFlexibility(double flexibility) {
	    this.flexibility = flexibility;
	}

	public double getFrequencyAllocated() {
	    return frequencyAllocated;
	}

	public void setFrequencyAllocated(double frequencyAllocated) {
	    this.frequencyAllocated = frequencyAllocated;
	}
    }

    public boolean isEnableDeadControl() {
	return enableDeadControl;
    }

    public void setEnableDeadControl(boolean disableDeadControl) {
	this.enableDeadControl = disableDeadControl;
    }

    @Override
    public void scheduleTasks() {
	//

    }

    @Override
    public void executeTasks() {
	//

    }

    @Override
    public void elaborateIncomingMessages() {
	//

    }

}