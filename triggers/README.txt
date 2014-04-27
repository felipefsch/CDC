First: you must have Cassandra with trigger patch.
You install as in https://issues.apache.org/jira/secure/attachment/12451779/HOWTO-PatchAndRunTriggerExample.txt

After building assure that everything works than stop Cassandra and do the follow:
-Change static variables of desired trigger to match your configuration
-Add trigger to contrib/trigger_example/src
-Include trigger specifications in the end of cassandra.yaml at contrib/trigger_example
-Run \'ant\' to build
-Start Cassandra again
-It must be working now		