package fi.aalto.itia.aggregator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import fi.aalto.itia.adr_em_common.ADR_EM_Common;
import fi.aalto.itia.util.Utility;

public class FrequencyProducer implements Runnable, Serializable {
    /**
	 * 
	 */
    private static final long serialVersionUID = 3212023433186094641L;
    private static final int FREQ_UPDATE = ADR_EM_Common.ONE_SECOND;
    private static final String FREQ_FILE = "FREQ_FILE";
    private static final String FILE_NAME_PROPERTIES = Aggregator.FILE_NAME_PROPERTIES;
    private static final String NOMINAL_FREQ_VALUE = "50.0";
    private static final String FREQ_FILE_NAME;

    /**
	 * 
	 */
    private static ArrayList<String> frequency = new ArrayList<String>();
    private static boolean keepGoing = true;
    private static AtomicReference<String> currentFreqValue = new AtomicReference<String>();

    private static int index = -1;
    private static FrequencyProducer gf;
    private static Thread freq_t;

    private boolean nominalModeOn = false;
    private boolean customModeOn = false;
    private String customFreq = NOMINAL_FREQ_VALUE;

    static {
	Properties properties = Utility.getProperties(FILE_NAME_PROPERTIES);
	FREQ_FILE_NAME = properties.getProperty(FREQ_FILE);
	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	BufferedReader br = null;
	String line;
	try {
	    br = new BufferedReader(new InputStreamReader(
		    classLoader.getResourceAsStream(FREQ_FILE_NAME)));
	    while ((line = br.readLine()) != null) {
		frequency.add(line);
	    }
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }

    private FrequencyProducer() {
	keepGoing = true;
	currentFreqValue.set(NOMINAL_FREQ_VALUE);
	index = -1;

    }

    // starts the singleton instance of the class (with start it means the
    // Thread itself)
    public static FrequencyProducer startInstance() {
	if (freq_t == null || gf == null || keepGoing == false) {
	    gf = new FrequencyProducer();
	    freq_t = new Thread(gf);
	    freq_t.start();
	}
	return gf;
    }

    @Override
    public void run() {
	while (keepGoing) {
	    if (index >= frequency.size() - 1) {
		index = -1;
	    }
	    if (this.nominalModeOn) {
		currentFreqValue.set(NOMINAL_FREQ_VALUE);
	    } else if (this.customModeOn) {
		currentFreqValue.set(customFreq);
	    } else {
		currentFreqValue.set(frequency.get(++index));
	    }

	    try {
		Thread.sleep(FREQ_UPDATE);
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	}

    }

    public boolean isNominalModeOn() {
	return nominalModeOn;
    }

    public void setNominalModeOn(boolean nominalModeOn) {
	this.nominalModeOn = nominalModeOn;
    }

    public static String getCurrentFreqValue() {
	if (index == -1) {
	    return NOMINAL_FREQ_VALUE;
	}
	return currentFreqValue.get();
    }

    public static boolean isKeepGoing() {
	return keepGoing;
    }

    public static void setKeepGoingToFalse() {
	index = -1;
	FrequencyProducer.keepGoing = false;
	gf = null;
	freq_t = null;
    }

    public boolean isCustomModeOn() {
	return customModeOn;
    }

    public void setCustomModeoff() {
	this.customModeOn = false;
    }

    public void setCustomModeOn(boolean customModeOn, String customFreq) {
	this.customModeOn = customModeOn;
	this.setCustomFreq(customFreq);
    }

    public String getCustomFreq() {
	return customFreq;
    }

    public void setCustomFreq(String customFreq) {
	this.customFreq = customFreq;
    }

}
