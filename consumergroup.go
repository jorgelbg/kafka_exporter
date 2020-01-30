package main

import (
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	plog "github.com/prometheus/common/log"
)

func (e *Exporter) getConsumerGroupMetrics(broker *sarama.Broker, config *sarama.Config, offset map[string]map[int32]int64, ch chan<- prometheus.Metric) {
	if err := broker.Open(config); err != nil && err != sarama.ErrAlreadyConnected {
		plog.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
		return
	}
	defer broker.Close()

	groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
	if err != nil {
		plog.Errorf("Cannot get consumer group: %v", err)
		return
	}
	groupIds := make([]string, 0)
	for groupId := range groups.Groups {
		if e.groupFilter.MatchString(groupId) != e.filterMode {
			groupIds = append(groupIds, groupId)
		}
	}

	describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
	if err != nil {
		plog.Errorf("Cannot get describe groups: %v", err)
		return
	}
	for _, group := range describeGroups.Groups {
		offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
		for topic, partitions := range offset {
			for partition := range partitions {
				offsetFetchRequest.AddPartition(topic, partition)
			}
		}
		ch <- prometheus.MustNewConstMetric(
			consumergroupMembers, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
		)
		if offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest); err != nil {
			plog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
		} else {
			for topic, partitions := range offsetFetchResponse.Blocks {
				// If the topic is not consumed by that consumer group, skip it
				topicConsumed := false
				for _, offsetFetchResponseBlock := range partitions {
					// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
					if offsetFetchResponseBlock.Offset != -1 {
						topicConsumed = true
						break
					}
				}
				if topicConsumed {
					var currentOffsetSum int64
					var lagSum int64
					for partition, offsetFetchResponseBlock := range partitions {
						err := offsetFetchResponseBlock.Err
						if err != sarama.ErrNoError {
							plog.Errorf("Error for  partition %d :%v", partition, err.Error())
							continue
						}
						currentOffset := offsetFetchResponseBlock.Offset
						currentOffsetSum += currentOffset
						ch <- prometheus.MustNewConstMetric(
							consumergroupCurrentOffset, prometheus.GaugeValue, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
						)
						e.mu.Lock()
						if offset, ok := offset[topic][partition]; ok {
							// If the topic is consumed by that consumer group, but no offset associated with the partition
							// forcing lag to -1 to be able to alert on that
							var lag int64
							if offsetFetchResponseBlock.Offset == -1 {
								lag = -1
							} else {
								lag = offset - offsetFetchResponseBlock.Offset
								lagSum += lag
							}
							ch <- prometheus.MustNewConstMetric(
								consumergroupLag, prometheus.GaugeValue, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
							)
						} else {
							plog.Errorf("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
						}
						e.mu.Unlock()
					}
					ch <- prometheus.MustNewConstMetric(
						consumergroupCurrentOffsetSum, prometheus.GaugeValue, float64(currentOffsetSum), group.GroupId, topic,
					)
					ch <- prometheus.MustNewConstMetric(
						consumergroupLagSum, prometheus.GaugeValue, float64(lagSum), group.GroupId, topic,
					)
				}
			}
		}
	}
}
