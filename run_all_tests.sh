RUNS=30
OPERATIONS=1000000

mkdir results

echo "Starting performance test" > results/log.txt

echo "Create schema" >> results/log.txt
# Create Cassandra Schema
java -jar CDC/Test.jar -prop CDC/CDC.properties -v -c

echo "Load data" >> results/log.txt
# Load Data
ycsb-0.1.4/bin/ycsb load cassandra-10 -P ycsb-0.1.4/workloads/workloada -p hosts=127.0.0.1 -p insertorder=ordered -p recordcount=$OPERATIONS -p operationcount=$OPERATIONS

echo "Add timestamp to config files" >> results/log.txt
# Append timestamp to properties files
java -jar CDC/Test.jar -timestamp >> CDC/CDC.properties
java -jar CDC/Test.jar -timestamp >> CDC/CDCSnapshot.properties

echo "Create first snapshot" >> results/log.txt
# Create First Snapshot of the Database
java -jar CDC/Test.jar -prop CDC/CDCSnapshot.properties -snapshot

echo "Update database" >> results/log.txt
# Update Data
ycsb-0.1.4/bin/ycsb run cassandra-10 -P ycsb-0.1.4/workloads/workloada -p hosts=127.0.0.1 -p insertorder=ordered -p recordcount=$OPERATIONS -p operationcount=$OPERATIONS

echo "STARTING TABLE SCAN!!!!!!!" >> results/log.txt

for x in $(seq 1 $RUNS)
do
	echo "Run " $x >> results/log.txt
	T1=$(echo $(date +%s%N))
	java -jar CDC/Test.jar -prop CDC/CDC.properties -tablescan
	T2=$(echo $(date +%s%N))

	echo "Run " $x " (ns) " $((T2-T1)) >> results/tablescan.txt
done

echo "TABLE SCAN DONE!!!!!!!"

echo "STARTING SNAPSHOT!!!!!!!" >> results/log.txt

for x in $(seq 1 $RUNS)
do
	echo "Run " $x >> results/log.txt
	T1=$(echo $(date +%s%N))
	java -jar CDC/Test.jar -prop CDC/CDC.properties -snapshot
	T2=$(echo $(date +%s%N))

	echo "Run " $x " (ns) " $((T2-T1)) >> results/snapshot.txt
done

echo "SNAPSHOT DONE!!!!!!!"

echo "STARTING DIFFERENTIAL!!!!!!!" >> results/log.txt

for x in $(seq 1 $RUNS)
do
	echo "Run " $x >> results/log.txt
	T1=$(echo $(date +%s%N))
	java -jar CDC/Test.jar -prop CDC/CDC.properties -differential
	T2=$(echo $(date +%s%N))

	echo "Run " $x " (ns) " $((T2-T1)) >> results/differential.txt
done

echo "DIFFERENTIAL DONE!!!!!!!"

echo "CREATING AND MAINTAINING TRACKING TABLE!!!!!!!" >> results/log.txt
java -jar CDC/Test.jar -prop CDC/CDC.properties -createtracking
java -jar CDC/Test.jar -prop CDC/CDC.properties -preparetracking
echo "TRACKING TABLE CREATED AND MAINTAINED!!!!!!!"

echo "STARTING TRACKING TABLE!!!!!!!" >> results/log.txt

for x in $(seq 1 $RUNS)
do
	echo "Run " $x >> results/log.txt
	T1=$(echo $(date +%s%N))
	java -jar CDC/Test.jar -prop CDC/CDC.properties -trackingtable
	T2=$(echo $(date +%s%N))

	echo "Run " $x " (ns) " $((T2-T1)) >> results/trackingtable.txt
done

echo "TRACKING TABLE DONE!!!!!!!"

echo "CREATING AUDIT COLUMNS!!!!!!!" >> results/log.txt
java -jar CDC/Test.jar -prop CDC/CDC.properties -createaudit
echo "AUDIT COLUMNS CREATED!!!!!!!"

echo "STARTING AUDIT COLUMN!!!!!!!" >> results/log.txt

for x in $(seq 1 $RUNS)
do
	echo "Run " $x >> results/log.txt
	T1=$(echo $(date +%s%N))
	java -jar CDC/Test.jar -prop CDC/CDC.properties -differential
	T2=$(echo $(date +%s%N))

	echo "Run " $x " (ns) " $((T2-T1)) >> results/auditcolumn.txt
done

echo "AUDIT COLUMN DONE!!!!!!!"

echo "FINISHED" >> results/log.txt
