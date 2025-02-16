# task-group-artifact

This repository contains the experiment software and logs pertaining to Maximizing Data Locality in HPC Workflows with Runtime Dependency Based Task Grouping

There is a directory for each application containing the logs of each run present in the evaluation along with the application which was used.

The each log filename has a prefix indicating the method used. "base" is the base TaskVine scheduler, "groups" is using Task Groups, and "htex" is using the Parsl HTEX executor without local storage.

Logs from TaskVine executions are directories providing multiple sources of data collected by taskvine which can be interpreted by TaskVine performance analysis tools.

Logs from Parsl runs without the TaskVine executor are simply the Parsl event log, since TaskVine was not used. 
