# This script sets the initial_token of Cassandras config file
# You must first give a base config file with everything setted up but
# the initial token
#
# A useful site to define Cassandra's initial tokens
# http://www.geroba.com/cassandra/cassandra-token-calculator/


HOSTS=(
0
1
2
3
4
6
7
9
13
14
18
19
)
KEYS=(
0
14178431955039102644307275309657008810
28356863910078205288614550619314017620
42535295865117307932921825928971026430
56713727820156410577229101238628035240
70892159775195513221536376548285044050
85070591730234615865843651857942052860
99249023685273718510150927167599061670
113427455640312821154458202477256070480
127605887595351923798765477786913079290
141784319550391026443072753096570088100
155962751505430129087380028406227096910
)

for k in $(seq 0 $((${#HOSTS[@]} - 1)))
do
        mkdir cassandra/conf.compute-0-${HOSTS[$k]}.local
        #touch "${HOSTS[$k]}"/"${KEYS[$k]}".txt
        cp default_conf/* cassandra/conf.compute-0-${HOSTS[$k]}.local/
        printf "\n%s\n" "initial_token: ${KEYS[$k]}" >> cassandra/conf.compute-0-${HOSTS[$k]}.local/cassandra.yaml
done
