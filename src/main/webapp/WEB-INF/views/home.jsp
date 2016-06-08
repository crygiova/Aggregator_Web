<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ page session="false"%>
<html>
<head>
<title>Simulation Aggregator Home</title>
</head>
<body>
	<h1>Hello CG!</h1>

	<h4>Aggregator Simulation Home Page</h4>

	<p>Aggregator Simulation Started: ${simStarted}</p>

	<a href="startAgg">Start Aggregator</a>
	<a href="stopAgg">Stop Aggregator</a>
	<a href="home">HOME</a>
	
	<P>Notes: ${notes}.</P>
	
	<P>The time on the server is ${serverTime}.</P>
	
	<a href="aggregatorStats" target="_blank">StatsAgg</a>
	<a href="frequency" target="_blank">Frequency Producer</a>
	<a href="freqNominalOn" target="_blank">FreQNominal ON or OFF</a>

</body>
</html>
