package fi.aalto.itia.aggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import fi.aalto.itia.adr_em_common.AgingADRConsumer;
import fi.aalto.itia.adr_em_common.InstructionsMessageContent;
import fi.aalto.itia.adr_em_common.SimulationMessageFactory;
import fi.aalto.itia.adr_em_common.UpdateMessageContent;
import fi.aalto.itia.util.Utility;

import org.junit.Test;

public class AggregatorTest {

    private static final double MAX_FCRN_FREQ_VARIATION = 0.1d;
    private static final double NOMINAL_FREQ = 50d;
    private static final double BOTTOM_FREQ = NOMINAL_FREQ - MAX_FCRN_FREQ_VARIATION;
    private static final double TOP_FREQ = NOMINAL_FREQ + MAX_FCRN_FREQ_VARIATION;
    private static final String FILE_NAME_PROPERTIES = "agg_config.properties";
    private static final String TARGET_FLEX = "TARGET_FLEX";
    private static final String FREQ_BAND = "FREQ_BAND";

    private static final double targetFlex = 220d;
    private static final double freqDeadBand = 0.02d;

    @Test
    public void testSortAlgorithm() {
	Aggregator a = Aggregator.getInstance();
	double mille = 100;
	ArrayList<UpdateMessageContent> umc = new ArrayList<UpdateMessageContent>();
	// umc.add(SimulationMessageFactory.getUpdateMessageContent(currentConsumption,
	// possibleCut, reactionTimeCut, timeCut, possibleIncrease,
	// reactionTimeIncrease, timeIncrease, consumerSender, aging))
	// XXX Users that can increase consumption
	// System.out.println(Double.compare(0d, 10d));
	// System.out.println(Double.compare(10d, 20d));
	// System.out.println(Double.compare(20d, 20d));
	// System.out.println(Double.compare(20d, 10d));
	// System.out.println(Double.compare(20d, 0d));

	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 5 * mille,
		5 * mille, "C0", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 6 * mille,
		6 * mille, "C1", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 7 * mille,
		7 * mille, "C2", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 8 * mille,
		8 * mille, "C3", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 9 * mille,
		9 * mille, "C4", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 1 * mille,
		1 * mille, "C5", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 2 * mille,
		2 * mille, "C6", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 3 * mille,
		3 * mille, "C7", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 4 * mille,
		4 * mille, "C8", new AgingADRConsumer(8, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(0d, 0d, 0d, 0d, 10d, 2 * mille,
		2 * mille, "C9", new AgingADRConsumer(8, 0)));

	// XXXUsers that can decrease consumption
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 1 * mille,
		0d, 0d, 0d, "C10", new AgingADRConsumer(0, 10)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 2 * mille, 2 * mille,
		0d, 0d, 0d, "C11", new AgingADRConsumer(0, 10)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 3 * mille,
		0d, 0d, 0d, "C12", new AgingADRConsumer(0, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 4 * mille,
		0d, 0d, 0d, "C13", new AgingADRConsumer(0, 20)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 4 * mille,
		0d, 0d, 0d, "C14", new AgingADRConsumer(0, 20)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 4 * mille,
		0d, 0d, 0d, "C15", new AgingADRConsumer(0, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 5 * mille,
		0d, 0d, 0d, "C16", new AgingADRConsumer(0, 30)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 6 * mille,
		0d, 0d, 0d, "C17", new AgingADRConsumer(0, 40)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 6 * mille, 6 * mille,
		0d, 0d, 0d, "C18", new AgingADRConsumer(0, 0)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 7 * mille, 7 * mille,
		0d, 0d, 0d, "C19", new AgingADRConsumer(0, 40)));
	umc.add(SimulationMessageFactory.getUpdateMessageContent(10d, 10d, 8 * mille, 8 * mille,
		0d, 0d, 0d, "C20", new AgingADRConsumer(0, 0)));
	elaborateInstructionsV2(umc);

    }

    public TreeMap<String, InstructionsMessageContent> elaborateInstructionsV2(
	    ArrayList<UpdateMessageContent> _collectionUpdate) {
	System.out.println("**********Elaborate Instructions********************");
	List<UpdateMessageContent> collectionUpdate = _collectionUpdate;
	// Sort by time cut Desc
	// shffle
	Collections.shuffle(collectionUpdate);
	Collections.sort(collectionUpdate, UpdateMessageContent.OrderForDwElabComparator);
	TreeMap<String, InstructionsMessageContent> outMsg = new TreeMap<String, InstructionsMessageContent>();

	// TODO TODO DELETE QUICK DEBUG
	// for (UpdateMessageContent updateMessageContent : collectionUpdate) {
	// log.info("ORDERED: " + updateMessageContent.toString());
	// }
	System.out.println("\n-------DownBuond ALGORITHM-------");
	for (UpdateMessageContent updateMessageContent : collectionUpdate) {
	    // if (updateMessageContent.getTimeCut() != 0d)
	    System.out.println(updateMessageContent.getConsumerSender() + " - DWnTimeCut: "
		    + updateMessageContent.getTimeCut() + " - DWnReactionCut: "
		    + updateMessageContent.getReactionTimeCut() + " - AgingDw "
		    + updateMessageContent.getAging().toString());
	}

	double targetFlexToCut = targetFlex;
	double freqActionBand = MAX_FCRN_FREQ_VARIATION - freqDeadBand;
	double band = NOMINAL_FREQ - freqDeadBand;
	double consumerPossibleCut;

	for (UpdateMessageContent updateMessageContent : collectionUpdate) {
	    // if the rest of the consumers cannot cut else cond in the loop
	    if (updateMessageContent.getTimeCut().compareTo(new Double(0d)) != 0
		    && updateMessageContent.getPossibleCut().compareTo(new Double(0d)) != 0
		    && targetFlexToCut > 0d) {
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
		outMsg.put(updateMessageContent.getConsumerSender(),
			new InstructionsMessageContent(updateMessageContent.getConsumerSender()));
	    }
	}

	// UPBUOND ALGORITHM
	System.out.println("\n-------UPBUOND ALGORITHM-------");
	// sort bu time increase
	Collections.sort(collectionUpdate, UpdateMessageContent.OrderForUpElabComparator);
	// TODO TODO DELETE QUICK DEBUG
	for (UpdateMessageContent updateMessageContent : collectionUpdate) {
	    // if (updateMessageContent.getTimeIncrease() != 0d)
	    System.out.println(updateMessageContent.getConsumerSender() + " UpTimeInc: "
		    + updateMessageContent.getTimeIncrease() + " UpReactInc: "
		    + updateMessageContent.getReactionTimeIncrease() + " - AgingDw "
		    + updateMessageContent.getAging().toString());
	}

	double targetFlexToIncrease = targetFlex;
	freqActionBand = MAX_FCRN_FREQ_VARIATION - freqDeadBand;
	band = NOMINAL_FREQ + freqDeadBand;
	double consumerPossibleIncrease;

	for (UpdateMessageContent updateMessageContent : collectionUpdate) {
	    // if the rest of the consumers cannot cut else cond in the loop
	    if (updateMessageContent.getTimeIncrease().compareTo(new Double(0d)) != 0
		    && updateMessageContent.getPossibleIncrease().compareTo(new Double(0d)) != 0
		    && targetFlexToIncrease > 0d) {
		// if we still have to reach the bottom_freq
		// TODO review the condition
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
		}
		// the other consumers will be used for FCR-D
		else {
		    band = TOP_FREQ;
		}

		InstructionsMessageContent imc = outMsg.get(updateMessageContent
			.getConsumerSender());
		imc.setAboveNominalFrequency(band);
		imc.setAboveNominalIncrease(updateMessageContent.getPossibleIncrease());
		// Instruction
		outMsg.put(updateMessageContent.getConsumerSender(), imc);
	    } else {
		// empty Instruction, taking into account the previous part of
		// the algorithm
		InstructionsMessageContent imc = outMsg.get(updateMessageContent
			.getConsumerSender());
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
	System.out.println("+++++++++++++++++++ TargetFlex = " + targetFlex);
	System.out.println("\nUnder Freq:");
	Collections.sort(imc, InstructionsMessageContent.DescSortByUnderFrequency);
	double count = 0d;
	for (InstructionsMessageContent instructionsMessageContent : imc) {
	    if (instructionsMessageContent.getUnderNominalFrequency() != 0)
		System.out.println(instructionsMessageContent.getConsumerReceiver() + " - "
			+ instructionsMessageContent.getUnderNominalFrequency() + " - "
			+ instructionsMessageContent.getUnderNominalDecrease());
	    count += instructionsMessageContent.getUnderNominalDecrease();
	}
	System.out.println("TOTAL FLEX UNDER : " + count);
	count = 0d;
	System.out.println("\nAbove Freq");
	Collections.sort(imc, InstructionsMessageContent.AscSortByAboveFrequency);

	for (InstructionsMessageContent instructionsMessageContent : imc) {
	    if (instructionsMessageContent.getAboveNominalFrequency() != 0)
		System.out.println(instructionsMessageContent.getConsumerReceiver() + " - "
			+ instructionsMessageContent.getAboveNominalFrequency() + " - "
			+ instructionsMessageContent.getAboveNominalIncrease());
	    count += instructionsMessageContent.getAboveNominalIncrease();
	}
	System.out.println("TOTAL FLEX Above : " + count);
	System.out.println("\n**********End Algorithm********************");
	return outMsg;
    }
}
