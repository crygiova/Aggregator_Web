package fi.aalto.itia;

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import fi.aalto.itia.adr_em_common.ADR_EM_Common;
import fi.aalto.itia.aggregator.Aggregator;
import fi.aalto.itia.aggregator.FrequencyProducer;
import fi.aalto.itia.aggregator.TaskAllocAggregator;

/**
 * Handles requests for the application home page.
 */
@Controller
public class AggController {

    private static final Logger logger = LoggerFactory.getLogger(AggController.class);

    private static String notes;
    private static boolean simulationStarted = false;

    // TODO CHange Start the frequency Thread
    private static FrequencyProducer freq;

    // Aggregator declarations
    private TaskAllocAggregator agg;
    private Thread t_agg;

    /**
     * Simply selects the home view to render by returning its name.
     */
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String home(@RequestParam(value = "cmd", required = false) String cmd, Locale locale,
	    Model model) {
	
	notes = "";
	if (agg == null) {
	    //agg = Aggregator.getNewInstance(ADR_EM_Common.AGG_INPUT_QUEUE);
	    agg = TaskAllocAggregator.getNewInstance(ADR_EM_Common.AGG_INPUT_QUEUE);
	}

	Date date = new Date();
	DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG,
		locale);
	String formattedDate = dateFormat.format(date);

	model.addAttribute("serverTime", formattedDate);
	model.addAttribute("notes", notes);
	model.addAttribute("simStarted", simulationStarted);
	// mapped jsp file
	logger.info("Welcome home! The client locale is {}.", locale);
	return "home";
    }

    @RequestMapping(value = "/startAgg", method = RequestMethod.GET)
    public String startAgg(Locale locale, Model model) {
	logger.info("Starting the Aggregator! The client locale is {}.", locale);
	if (agg == null) {
	    //agg = Aggregator.getNewInstance(ADR_EM_Common.AGG_INPUT_QUEUE);
	    agg = TaskAllocAggregator.getNewInstance(ADR_EM_Common.AGG_INPUT_QUEUE);
	}
	if (!simulationStarted) {
	    // start simulation
	    // Start Frequency Producer
	    freq = FrequencyProducer.startInstance();
	    t_agg = new Thread(agg);
	    t_agg.start();
	    simulationStarted = true;
	    logger.debug("Aggregator Simulation Started");
	} else {
	    // simulation already started
	    notes += "Simulation Already Started";
	    logger.debug("Simulation Already Started");
	}
	return "redirect:";
    }

    @RequestMapping(value = "/stopAgg", method = RequestMethod.GET)
    public String stopAgg(Locale locale, Model model) {
	logger.info("Starting the Aggregator! The client locale is {}.", locale);
	if (simulationStarted) {
	    // stop simulation
	    agg.setKeepGoing(false);
	    simulationStarted = false;
	    // once the a simulation is over it returns a new aggregator
	    //agg = Aggregator.getNewInstance(ADR_EM_Common.AGG_INPUT_QUEUE);
	    agg = TaskAllocAggregator.getNewInstance(ADR_EM_Common.AGG_INPUT_QUEUE);
	    logger.debug("Aggregator Simulation Stopped");
	    // STop Frequency PRoducer
	    FrequencyProducer.setKeepGoingToFalse();
	} else {
	    // simulation already already stopped
	    notes += "Simulation Already Stopped";
	    logger.debug("Simulation Already Stopped");
	}
	return "redirect:";
    }

    @RequestMapping(value = "/statsAgg", method = RequestMethod.GET)
    public String statsAgg(Locale locale, Model model) {

	if (agg != null) {
	    model.addAttribute("nCons", agg.getConsumersSize());
	}

	return "statsAgg";
    }

    @RequestMapping(value = "/freqNominalOn", method = RequestMethod.GET)
    public @ResponseBody String freqNominalOn() {
	freq.setCustomModeoff();
	freq.setNominalModeOn(!this.freq.isNominalModeOn());
	return "NominalMode ON: " + this.freq.isNominalModeOn();
    }
    
    //enables and disables deadControl ni TaskAllocAggr
    @RequestMapping(value = "/enableDead", method = RequestMethod.GET)
    public @ResponseBody String enableDead() {
	boolean negate = ! ((TaskAllocAggregator) agg).isEnableDeadControl();
	((TaskAllocAggregator) agg).setEnableDeadControl(negate);
	return "DeadControl ON: " + negate;
    }

    @RequestMapping(value = "/freqCustomOn/{someID}", method = RequestMethod.GET)
    public @ResponseBody String freqCustomOn(@PathVariable(value = "someID") String custom) {
	custom = custom.replace("p", ".");
	freq.setNominalModeOn(false);
	freq.setCustomModeOn(true, custom);
	return "CustomMode ON: " + this.freq.isCustomModeOn() + " --Value:" + custom;
    }

    @RequestMapping(value = "/frequency", method = RequestMethod.GET)
    @ResponseBody
    public String frequency(Locale locale, Model model) {
	return FrequencyProducer.getCurrentFreqValue();
    }

}
