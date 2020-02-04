package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	plog "github.com/prometheus/common/log"
)

func getPartitionMetrics(c sarama.Client, topic string, partition int32, ch chan<- prometheus.Metric) int64 {
	now := time.Now()

	broker, err := c.Leader(topic, partition)
	if err != nil {
		plog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
	} else {
		ch <- prometheus.MustNewConstMetric(
			topicPartitionLeader, prometheus.GaugeValue, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
		)
	}
	plog.Debugf("%s to get leader on %s %d ", time.Now().Sub(now), topic, partition)

	currentOffset, err := c.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		plog.Errorf("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
	} else {
		ch <- prometheus.MustNewConstMetric(
			topicCurrentOffset, prometheus.GaugeValue, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10),
		)
	}
	plog.Debugf("%s to get offset on %s %d ", time.Now().Sub(now), topic, partition)

	oldestOffset, err := c.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		plog.Errorf("Cannot get oldest offset of topic %s partition %d: %v", topic, partition, err)
	} else {
		ch <- prometheus.MustNewConstMetric(
			topicOldestOffset, prometheus.GaugeValue, float64(oldestOffset), topic, strconv.FormatInt(int64(partition), 10),
		)
	}
	plog.Debugf("%s to get oldest offset on %s %d ", time.Now().Sub(now), topic, partition)

	replicas, err := c.Replicas(topic, partition)
	if err != nil {
		plog.Errorf("Cannot get replicas of topic %s partition %d: %v", topic, partition, err)
	} else {
		ch <- prometheus.MustNewConstMetric(
			topicPartitionReplicas, prometheus.GaugeValue, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
		)
	}
	plog.Debugf("%s to get replicas on %s %d ", time.Now().Sub(now), topic, partition)

	inSyncReplicas, err := c.InSyncReplicas(topic, partition)
	if err != nil {
		plog.Errorf("Cannot get in-sync replicas of topic %s partition %d: %v", topic, partition, err)
	} else {
		ch <- prometheus.MustNewConstMetric(
			topicPartitionInSyncReplicas, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
		)
	}
	plog.Debugf("%s to get in sync replicas on %s %d ", time.Now().Sub(now), topic, partition)

	if broker != nil && replicas != nil && len(replicas) > 0 && broker.ID() == replicas[0] {
		ch <- prometheus.MustNewConstMetric(
			topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
		)
	} else {
		ch <- prometheus.MustNewConstMetric(
			topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
		)
	}

	if replicas != nil && inSyncReplicas != nil && len(inSyncReplicas) < len(replicas) {
		ch <- prometheus.MustNewConstMetric(
			topicUnderReplicatedPartition, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
		)
	} else {
		ch <- prometheus.MustNewConstMetric(
			topicUnderReplicatedPartition, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
		)
	}
	return currentOffset
}

func (e *Exporter) getTopicMetrics(topic string, offsetChan chan<- topicOffset, ch chan<- prometheus.Metric) {
	// offset map[string]map[int32]int64
	if e.topicFilter.MatchString(topic) == e.filterMode {
		return
	}
	tOff := topicOffset{Name: topic}
	now := time.Now()
	c := <-e.clientPool
	defer func() { e.clientPool <- c }()

	partitions, err := c.Partitions(topic)
	if err != nil {
		plog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(
		topicPartitions, prometheus.GaugeValue, float64(len(partitions)), topic,
	)

	plog.Debugf("%s to match topic", time.Now().Sub(now))
	tOff.Partitions = make(map[int32]int64, len(partitions))

	wg := sync.WaitGroup{}
	for _, partition := range partitions {
		wg.Add(1)
		go func(partition int32) {

			curr := getPartitionMetrics(c, topic, partition, ch)
			e.mu.Lock()
			tOff.Partitions[partition] = curr
			e.mu.Unlock()
			wg.Done()
		}(partition)
	}
	wg.Wait()

	if e.useZooKeeperLag {
		for _, partition := range partitions {
			consumerGroups, err := e.zkClient.Consumergroups()

			if err != nil {
				plog.Errorf("Cannot get consumer group %v", err)
			}

			for _, group := range consumerGroups {
				offset, _ := group.FetchOffset(topic, partition)
				if offset > 0 {

					consumerGroupLag := tOff.Partitions[partition] - offset
					ch <- prometheus.MustNewConstMetric(
						consumergroupLagZookeeper, prometheus.GaugeValue, float64(consumerGroupLag), group.Name, topic, strconv.FormatInt(int64(partition), 10),
					)
				}
			}
		}
	}

	offsetChan <- tOff
}
