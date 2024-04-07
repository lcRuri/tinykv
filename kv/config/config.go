package config

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"os"
	"time"
)

type Config struct {
	StoreAddr     string //存储地址
	Raft          bool   //??
	SchedulerAddr string //调度地址
	LogLevel      string //日志级别

	DBPath string //具体存储数据的地址 必须存在并且可以写入

	//raft_base_tick_interval 是基本刻度间隔 （ms）。
	RaftBaseTickInterval     time.Duration //raft基本的tick间隔
	RaftHeartbeatTicks       int           //raft心跳tick
	RaftElectionTimeoutTicks int           //raft选举超时时间tick

	// 垃圾回收的间隔
	RaftLogGCTickInterval time.Duration

	// gc回收的触发大小
	RaftLogGcCountLimit uint64

	//region split 检查时间间隔
	SplitRegionCheckTickInterval time.Duration

	//在删除过时的节点 延迟时间 ??
	SchedulerHeartbeatTickInterval      time.Duration
	SchedulerStoreHeartbeatTickInterval time.Duration

	//最大容量和分割容量 当区域 [a，e） 大小满足 regionMaxSize 时，它将被拆分为
	//	几个区域 [a、b、c、d]、[d、e]。和 [a，b] 的大小，
	//	[b，c）， [c，d） 将是 regionSplitSize（可能稍大一点）。
	RegionMaxSize   uint64
	RegionSplitSize uint64
}

// Validate 满足了检验的接口
func (c *Config) Validate() error {
	//心跳时间要大于0
	if c.RaftHeartbeatTicks == 0 {
		return fmt.Errorf("heartbeat tick must greater than 0")
	}

	if c.RaftElectionTimeoutTicks != 10 {
		//所有集群的选举超时需要相同否则可能会导致不一致 这个在config里面默认是10
		log.Warnf("Election timeout ticks needs to be same across all the cluster, " +
			"otherwise it may lead to inconsistency.")
	}

	if c.RaftElectionTimeoutTicks <= c.RaftHeartbeatTicks {
		return fmt.Errorf("election tick must be greater than heartbeat tick.")
	}

	return nil
}

const (
	KB uint64 = 1024
	MB uint64 = 1024 * 1024
)

func NewDefaultConfig() *Config {
	return &Config{
		SchedulerAddr:            "127.0.0.1:2379",
		StoreAddr:                "127.0.0.1:20160",
		LogLevel:                 getLogLevel(),
		Raft:                     true,
		RaftBaseTickInterval:     1 * time.Second,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    10 * time.Second,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        10 * time.Second,
		SchedulerHeartbeatTickInterval:      10 * time.Second,
		SchedulerStoreHeartbeatTickInterval: 10 * time.Second,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}

func NewTestConfig() *Config {
	return &Config{
		LogLevel:                 getLogLevel(),
		Raft:                     true,
		RaftBaseTickInterval:     50 * time.Millisecond,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    50 * time.Millisecond,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        100 * time.Millisecond,
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond,
		SchedulerStoreHeartbeatTickInterval: 500 * time.Millisecond,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}

func getLogLevel() (logLevel string) {
	logLevel = "info"
	//读取环境变量中是否有值
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		logLevel = l
	}

	return

}
