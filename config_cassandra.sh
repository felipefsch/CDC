# This script sets the initial_token of Cassandras config file
# You must first give a base config file with everything setted up but
# the initial token
#
# A handful site to define Cassandra's initial tokens
# http://www.geroba.com/cassandra/cassandra-token-calculator/


HOSTS=( 1 2 4 )
KEYS=( 1 1344 112344 )

for k in $(seq 0 $((${#HOSTS[@]} - 1)))
do
        mkdir ${HOSTS[$k]}
        #touch "${HOSTS[$k]}"/"${KEYS[$k]}".txt
        cp hosts.txt "${HOSTS[$k]}"/
        echo "initial_token = ${KEYS[$k]}" >> "${HOSTS[$k]}"/hosts.txt
done
