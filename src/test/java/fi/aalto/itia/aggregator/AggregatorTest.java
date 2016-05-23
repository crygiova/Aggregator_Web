package fi.aalto.itia.aggregator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import fi.aalto.itia.adr_em_common.InstructionsMessageContent;
import fi.aalto.itia.adr_em_common.SimulationMessageFactory;
import fi.aalto.itia.adr_em_common.UpdateMessageContent;
import fi.aalto.itia.util.Utility;

import org.junit.Test;

public class AggregatorTest {

	private static final double MAX_FCRN_FREQ_VARIATION = 0.1d;
	private static final double NOMINAL_FREQ = 50d;
	private static final double BOTTOM_FREQ = NOMINAL_FREQ
			- MAX_FCRN_FREQ_VARIATION;
	private static final double TOP_FREQ = NOMINAL_FREQ
			+ MAX_FCRN_FREQ_VARIATION;
	private static final String FILE_NAME_PROPERTIES = "agg_config.properties";
	private static final String TARGET_FLEX = "TARGET_FLEX";
	private static final String FREQ_BAND = "FREQ_BAND";

	private static final double targetFlex = 220d;
	private static final double freqDeadBand = 0.02d;

	/*@Test
	public void testAllocationAlgorithm() {
		ArrayList<UpdateMessageContent> um = new ArrayList<UpdateMessageContent>();
		double min = 60d;
		um.add(SimulationMessageFactory.getUpdateMessage(10d, 10d,
				50 * min, 0d, 0d, "1"));
		um.add(SimulationMessageFactory.getUpdateMessage(20d, 2d,
				40 * min, 0d, 0d, "2"));
		um.add(SimulationMessageFactory.getUpdateMessage(30d, 3d,
				30 * min, 0d, 0d, "3"));
		um.add(SimulationMessageFactory.getUpdateMessage(40d, 4d,
				20 * min, 0d, 0d, "4"));
		um.add(SimulationMessageFactory.getUpdateMessage(10d, 10d,
				60 * min, 0d, 0d, "5"));
		um.add(SimulationMessageFactory.getUpdateMessage(60d, 6d,
				50 * min, 0d, 0d, "6"));
		um.add(SimulationMessageFactory.getUpdateMessage(70d, 7d,
				40 * min, 0d, 0d, "7"));
		um.add(SimulationMessageFactory.getUpdateMessage(80d, 8d,
				30 * min, 0d, 0d, "8"));
		um.add(SimulationMessageFactory.getUpdateMessage(90d, 9d,
				20 * min, 0d, 0d, "9"));
		um.add(SimulationMessageFactory.getUpdateMessage(10d, 10d,
				60 * min, 0d, 0d, "0"));

		TreeMap<String, InstructionsMessageContent> out = elaborateInstructions(um);
		ArrayList<InstructionsMessageContent> imc = new ArrayList<InstructionsMessageContent>(
				out.values());
		// for (InstructionsMessageContent instructionsMessageContent : imc) {
		// System.out.println(instructionsMessageContent.toString());
		// }
		System.out.println("FINAL RESULT Target = " + targetFlex);
		Collections.sort(imc,
				InstructionsMessageContent.DescSortByUnderFrequency);
		double count = 0d;
		for (InstructionsMessageContent instructionsMessageContent : imc) {
			System.out.println(instructionsMessageContent.getConsumerReceiver()
					+ " - "
					+ instructionsMessageContent.getUnderNominalFrequency()
					+ " - "
					+ instructionsMessageContent.getUnderNominalDecrease());
			count += instructionsMessageContent.getUnderNominalDecrease();
		}
		System.out.println("TOTAL FLEX : " + count);

	}*/

	private TreeMap<String, InstructionsMessageContent> elaborateInstructions(
			ArrayList<UpdateMessageContent> _collectionUpdate) {
		// frequency down or
		// TODO sort one side, sort another side
		// TODO think about the options you have
		List<UpdateMessageContent> collectionUpdate = _collectionUpdate;
		Collections.sort(collectionUpdate,
				UpdateMessageContent.DescSortByTimeCutComparator);
		TreeMap<String, InstructionsMessageContent> outMsg = new TreeMap<String, InstructionsMessageContent>();

		// for (UpdateMessageContent updateMessageContent : collectionUpdate) {
		// System.out.println("ORDERED: " + updateMessageContent.toString());
		// }
		for (UpdateMessageContent updateMessageContent : collectionUpdate) {
			System.out.println(updateMessageContent.getConsumerSender()
					+ " - ORD_BY_TIME_CUT: "
					+ updateMessageContent.getTimeCut());
		}

		double targetFlexToCut = targetFlex;
		double freqActionBand = MAX_FCRN_FREQ_VARIATION - freqDeadBand;
		double band = 50d - freqDeadBand;
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
				// Instruction
				outMsg.put(
						updateMessageContent.getConsumerSender(),
						new InstructionsMessageContent(updateMessageContent
								.getPossibleCut(), band, 0d, 0d,
								updateMessageContent.getConsumerSender()));
			} else {
				// empty Instruction
				outMsg.put(
						updateMessageContent.getConsumerSender(),
						new InstructionsMessageContent(updateMessageContent
								.getConsumerSender()));
			}
		}
		return outMsg;
		// TODO similar for upboundFrequency
	}

}
