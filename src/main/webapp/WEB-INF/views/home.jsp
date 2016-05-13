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

	<a href="http://localhost:8080/itia/startAgg">Start Aggregator</a>
	<a href="http://localhost:8080/itia/stopAgg">Stop Aggregator</a>
	<a href="http://localhost:8080/itia">HOME</a>
	
	<P>Notes: ${notes}.</P>
	
	<P>The time on the server is ${serverTime}.</P>
	
	<a href="http://localhost:8080/itia/statsAgg" target="_blank">StatsAgg</a>
	<a href="http://localhost:8080/itia/frequency" target="_blank">Frequency</a>

</body>
</html>
