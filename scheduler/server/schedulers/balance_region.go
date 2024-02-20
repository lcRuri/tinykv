// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
// BalanceRegionCreateOption 用于创建带有选项的调度程序
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	//选择要移动的区域 pending->follower->leader-> smaller region size
	// 先选store 再选region
	stores := cluster.GetStores()
	tmpStore := make([]*core.StoreInfo, 0)
	for _, store := range stores {
		//In short, a suitable store should be up and the down time cannot be longer than `MaxStoreDownTime` of the cluster, which you can get through `cluster.GetMaxStoreDownTime()`.
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			tmpStore = append(tmpStore, store)
		}
	}

	if len(tmpStore) <= 1 {
		return nil
	}

	// sort to find max and min store size
	sort.Slice(tmpStore, func(i, j int) bool {
		return tmpStore[i].GetRegionSize() > tmpStore[j].GetRegionSize()
	})

	originStore := &core.StoreInfo{}
	var pendingRegionInfo *core.RegionInfo
	var followerRegionInfo *core.RegionInfo
	var leaderRegionInfo *core.RegionInfo

	// it will try to select a pending region because pending may mean the disk is overloaded.
	for _, storeInfo := range tmpStore {
		cluster.GetPendingRegionsWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
			pendingRegionInfo = container.RandomRegion(nil, nil)
		})

		if pendingRegionInfo != nil {
			originStore = storeInfo
			break
		}
	}

	if pendingRegionInfo == nil {
		//find follower region
		for _, storeInfo := range tmpStore {
			cluster.GetFollowersWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
				followerRegionInfo = container.RandomRegion(nil, nil)
			})

			if followerRegionInfo != nil {
				originStore = storeInfo
				break
			}
		}

		if followerRegionInfo == nil {
			for _, storeInfo := range tmpStore {
				cluster.GetLeadersWithLock(storeInfo.GetID(), func(container core.RegionsContainer) {
					leaderRegionInfo = container.RandomRegion(nil, nil)
				})

				if leaderRegionInfo != nil {
					originStore = storeInfo
					break
				}
			}
		}
	}

	var moveRegion *core.RegionInfo
	if pendingRegionInfo != nil {
		moveRegion = pendingRegionInfo
	} else if followerRegionInfo != nil {
		moveRegion = followerRegionInfo
	} else if leaderRegionInfo != nil {
		moveRegion = leaderRegionInfo
	}

	if moveRegion == nil {
		return nil
	}

	//Actually, the Scheduler will select the store with the smallest region size
	// todo

	//make sure that the difference has to be bigger than two times the approximate size of the region
	targetStore := tmpStore[len(tmpStore)-1]
	if originStore.GetRegionSize()-targetStore.GetRegionSize() <= 2*moveRegion.GetApproximateSize() {
		return nil
	}

	//If the difference is big enough, the Scheduler should allocate a new peer on the target store
	allocPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		return nil
	}

	//and create a move peer operator.
	op, err := operator.CreateMovePeerOperator("balance-move", cluster, moveRegion, operator.OpBalance, originStore.GetID(), targetStore.GetID(), allocPeer.GetId())
	if err != nil {
		return nil
	}

	return op
}
