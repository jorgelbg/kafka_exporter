package main

import (
	"strconv"
	"sync"
	"time"

	kazoo "github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	plog "github.com/prometheus/common/log"
)

// FetchZookeeperLag gets and calculate the offset of zookeeper based consumers
func (e *Exporter) FetchZookeeperLag(ch chan<- prometheus.Metric, topicOffsets map[string]map[int32]int64) {
	consumerGroups, err := e.zkClient.Consumergroups()
	plog.Infof("consumerGroups=%d", len(consumerGroups))

	if err != nil {
		plog.Errorf("Cannot get consumer groups from Zookeeper: %v", err)
		return
	}

	start := time.Now()

	wg := sync.WaitGroup{}
	for _, group := range consumerGroups {
		wg.Add(1)
		go func(group *kazoo.Consumergroup) {
			offsets, err := group.FetchAllOffsets()
			if err != nil {
				plog.Errorf("Cannot get zookeeper offsets: %v", err)
				return
			}

			for topic, m := range offsets {
				for partition, offset := range m {
					// topicOffset - consumerOffset = lag
					lag := topicOffsets[topic][partition] - offset
					ch <- prometheus.MustNewConstMetric(
						consumergroupLagZookeeper, prometheus.GaugeValue,
						float64(lag), group.Name, topic, strconv.FormatInt(int64(partition), 10),
					)
				}
			}
			wg.Done()
		}(group)
	}
	wg.Wait()

	plog.Infof("Took: %dms", time.Now().Sub(start).Milliseconds())
}
