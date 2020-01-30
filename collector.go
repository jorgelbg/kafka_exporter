package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	plog "github.com/prometheus/common/log"
)

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- clusterBrokers
	ch <- topicCurrentOffset
	ch <- topicOldestOffset
	ch <- topicPartitions
	ch <- topicPartitionLeader
	ch <- topicPartitionReplicas
	ch <- topicPartitionInSyncReplicas
	ch <- topicPartitionUsesPreferredReplica
	ch <- topicUnderReplicatedPartition
	ch <- consumergroupCurrentOffset
	ch <- consumergroupCurrentOffsetSum
	ch <- consumergroupLag
	ch <- consumergroupLagZookeeper
	ch <- consumergroupLagSum
}

type topicOffset struct {
	Name       string          // Name of topic
	Partitions map[int32]int64 // Map of partitionIDs to Offset
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		clusterBrokers, prometheus.GaugeValue, float64(len(e.client.Brokers())),
	)

	offset := make(map[string]map[int32]int64)

	topics, err := e.client.Topics()
	if err != nil {
		plog.Errorf("Cannot get topics: %v", err)
		return
	}

	var wg = sync.WaitGroup{}
	rc := make(chan topicOffset, len(topics))
	for _, topic := range topics {
		if e.topicFilter.MatchString(topic) {
			wg.Add(1)
			go func(topic string) {
				e.getTopicMetrics(topic, rc, ch)
				wg.Done()
			}(topic)
		}
	}
	wg.Wait()
	close(rc)
	e.mu.Lock()
	for to := range rc {
		offset[to.Name] = to.Partitions
	}
	e.mu.Unlock()
	plog.Debugf("Took %s to get topic metrics", time.Now().Sub(now))
	now = time.Now()

	if len(e.client.Brokers()) > 0 {
		for _, broker := range e.client.Brokers() {
			wg.Add(1)
			go func(broker *sarama.Broker) {
				e.getConsumerGroupMetrics(broker, e.client.Config(), offset, ch)
				wg.Done()
			}(broker)
		}
		wg.Wait()
	} else {
		plog.Errorln("No valid broker, cannot get consumer group metrics")
	}
	plog.Debugf("Took %s to get group metrics", time.Now().Sub(now))
}
