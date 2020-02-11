# explore-kafka
explore kafka

# Normal kafka commands

Create topic `test` with 3 replication
    bin/kafka-topics.sh --create --zookeeper localhost:2181 \
      --replication-factor 3 --partitions 1 --topic test

Check topics

    bin/kafka-topics.sh --list --zookeeper localhost:2181 --topic test

    bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test

simple start environment

    docker build -t spark-kafka .
    
    docker run -d -p 2181:2181 -p 9092:9092 -v `pwd`:/data spark-kakfa