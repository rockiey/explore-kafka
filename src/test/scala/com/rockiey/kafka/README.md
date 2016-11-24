
Download kafka and unzip it

    bin/zookeeper-server-start.sh config/zookeeper.properties &
    bin/kafka-server-start.sh config/server.properties &

    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
    bin/kafka-topics.sh --list --zookeeper localhost:2181
