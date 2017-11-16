# evfs-lite
This project leverages the Google Cloud Platform to analyze large amounts of OpenXC data using Apache Beam (Google Dataflow),
Google Biguery, and Google Cloud Storage.

## Purpose
The purpose of EVFS-lite (EV Fleet Stats lite) is to analyze the usage behavior of electric vehicles.
Specifically, the goal is to look for a number of indicators, including (but limited to):

* Vehicle miles traveled (odometer)
  * Electric vehicle miles traveled (eVMT)
  * Fuel (gas/diesel/etc) miles traveled
* Endpoints (destinations) and their proximity to charging stations
* Fuel consumption (for PHEVs and ICEVs)
* Electric power consumption

Using this data, we can look at fleets of vehicles to determine:

* If drivers are charging vehicles (PHEVs)
* If drivers are charging at destinations (for PEVs)
  * If so, what charging mode (level 1,2,3) and cost
  * If not, determine the following
    * If they are close to a functioning charging station (missed charging opportunity)
    * If the nearby charging station isn't working (infrastructure fault)
    * If there is no charging station reasonably close to the destination (impossible to charge)
* Evaluate fueling and costs
* Calculate fuel savings from charging
* Calculate missed fuel savings from missed charging opportunities

These findings can be used to make procurement recommendations to fleet owners and infrastructure recommendations to facility operators.

## Use
Initially, this software is being used to evaluate the fleet of Ford Fusion Energi PHEVs at UC Davis.
However, the end goal is to create a tool that can be used for any fleet on any platform.
While Google Dataflow is the primary statistical analysis engine, Dataflow is simply an implementation of Beam.
Because Beam can be installed on any server and use Spark as a backend,
it is feasible that the code could be agnostic enough for any platform (e.g. AWS, a homebrew server farm, etc.).
Google Cloud Platform is the main target for now due to the ease of resource allocation.
Google's Cloud Platform was also chosen because BigQuery is a convenient way to store and query very large datasets that might otherwise quickly impact the performance of a MySQL or Postgres seOrver.
Some signals from the vehicles in question poll at a rate of up to 10ms. This may happen for up to 20 different signals.
As a result, the signal events from even one vehicle can very quickly scale to millions or billions of records, requiring the need for a big data storage solution.
