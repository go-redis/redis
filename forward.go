package redis

import (
	"log"
	"time"

	"github.com/go-redis/redis/v7"
)

// generated by goforward -filename forward.go github.com/go-redis/redis/v7 github.com/go-redis/redis

// ClusterOptions are used to configure a cluster client and should be
// passed to NewClusterClient.
type ClusterOptions = redis.ClusterOptions

// ClusterClient is a Redis Cluster client representing a pool of zero
// or more underlying connections. It's safe for concurrent use by
// multiple goroutines.
type ClusterClient = redis.ClusterClient

// NewClusterClient returns a Redis Cluster client as described in
// http://redis.io/topics/cluster-spec.
func NewClusterClient(opt *ClusterOptions) *ClusterClient {
	return redis.NewClusterClient(opt)
}

type Cmder = redis.Cmder

type Cmd = redis.Cmd

func NewCmd(args ...interface{}) *Cmd {
	return redis.NewCmd(args...)
}

type SliceCmd = redis.SliceCmd

func NewSliceCmd(args ...interface{}) *SliceCmd {
	return redis.NewSliceCmd(args...)
}

type StatusCmd = redis.StatusCmd

func NewStatusCmd(args ...interface{}) *StatusCmd {
	return redis.NewStatusCmd(args...)
}

type IntCmd = redis.IntCmd

func NewIntCmd(args ...interface{}) *IntCmd {
	return redis.NewIntCmd(args...)
}

type IntSliceCmd = redis.IntSliceCmd

func NewIntSliceCmd(args ...interface{}) *IntSliceCmd {
	return redis.NewIntSliceCmd(args...)
}

type DurationCmd = redis.DurationCmd

func NewDurationCmd(precision time.Duration, args ...interface{}) *DurationCmd {
	return redis.NewDurationCmd(precision, args...)
}

type TimeCmd = redis.TimeCmd

func NewTimeCmd(args ...interface{}) *TimeCmd {
	return redis.NewTimeCmd(args...)
}

type BoolCmd = redis.BoolCmd

func NewBoolCmd(args ...interface{}) *BoolCmd {
	return redis.NewBoolCmd(args...)
}

type StringCmd = redis.StringCmd

func NewStringCmd(args ...interface{}) *StringCmd {
	return redis.NewStringCmd(args...)
}

type FloatCmd = redis.FloatCmd

func NewFloatCmd(args ...interface{}) *FloatCmd {
	return redis.NewFloatCmd(args...)
}

type StringSliceCmd = redis.StringSliceCmd

func NewStringSliceCmd(args ...interface{}) *StringSliceCmd {
	return redis.NewStringSliceCmd(args...)
}

type BoolSliceCmd = redis.BoolSliceCmd

func NewBoolSliceCmd(args ...interface{}) *BoolSliceCmd {
	return redis.NewBoolSliceCmd(args...)
}

type StringStringMapCmd = redis.StringStringMapCmd

func NewStringStringMapCmd(args ...interface{}) *StringStringMapCmd {
	return redis.NewStringStringMapCmd(args...)
}

type StringIntMapCmd = redis.StringIntMapCmd

func NewStringIntMapCmd(args ...interface{}) *StringIntMapCmd {
	return redis.NewStringIntMapCmd(args...)
}

type StringStructMapCmd = redis.StringStructMapCmd

func NewStringStructMapCmd(args ...interface{}) *StringStructMapCmd {
	return redis.NewStringStructMapCmd(args...)
}

type XMessage = redis.XMessage

type XMessageSliceCmd = redis.XMessageSliceCmd

func NewXMessageSliceCmd(args ...interface{}) *XMessageSliceCmd {
	return redis.NewXMessageSliceCmd(args...)
}

type XStream = redis.XStream

type XStreamSliceCmd = redis.XStreamSliceCmd

func NewXStreamSliceCmd(args ...interface{}) *XStreamSliceCmd {
	return redis.NewXStreamSliceCmd(args...)
}

type XPending = redis.XPending

type XPendingCmd = redis.XPendingCmd

func NewXPendingCmd(args ...interface{}) *XPendingCmd {
	return redis.NewXPendingCmd(args...)
}

type XPendingExt = redis.XPendingExt

type XPendingExtCmd = redis.XPendingExtCmd

func NewXPendingExtCmd(args ...interface{}) *XPendingExtCmd {
	return redis.NewXPendingExtCmd(args...)
}

type ZSliceCmd = redis.ZSliceCmd

func NewZSliceCmd(args ...interface{}) *ZSliceCmd {
	return redis.NewZSliceCmd(args...)
}

type ZWithKeyCmd = redis.ZWithKeyCmd

func NewZWithKeyCmd(args ...interface{}) *ZWithKeyCmd {
	return redis.NewZWithKeyCmd(args...)
}

type ScanCmd = redis.ScanCmd

func NewScanCmd(process func(cmd Cmder) error, args ...interface{}) *ScanCmd {
	return redis.NewScanCmd(process, args...)
}

type ClusterNode = redis.ClusterNode

type ClusterSlot = redis.ClusterSlot

type ClusterSlotsCmd = redis.ClusterSlotsCmd

func NewClusterSlotsCmd(args ...interface{}) *ClusterSlotsCmd {
	return redis.NewClusterSlotsCmd(args...)
}

// GeoLocation is used with GeoAdd to add geospatial location.
type GeoLocation = redis.GeoLocation

// GeoRadiusQuery is used with GeoRadius to query geospatial index.
type GeoRadiusQuery = redis.GeoRadiusQuery

type GeoLocationCmd = redis.GeoLocationCmd

func NewGeoLocationCmd(q *GeoRadiusQuery, args ...interface{}) *GeoLocationCmd {
	return redis.NewGeoLocationCmd(q, args...)
}

type GeoPos = redis.GeoPos

type GeoPosCmd = redis.GeoPosCmd

func NewGeoPosCmd(args ...interface{}) *GeoPosCmd {
	return redis.NewGeoPosCmd(args...)
}

type CommandInfo = redis.CommandInfo

type CommandsInfoCmd = redis.CommandsInfoCmd

func NewCommandsInfoCmd(args ...interface{}) *CommandsInfoCmd {
	return redis.NewCommandsInfoCmd(args...)
}

type Cmdable = redis.Cmdable

type StatefulCmdable = redis.StatefulCmdable

type Sort = redis.Sort

type BitCount = redis.BitCount

type XAddArgs = redis.XAddArgs

type XReadArgs = redis.XReadArgs

type XReadGroupArgs = redis.XReadGroupArgs

type XPendingExtArgs = redis.XPendingExtArgs

type XClaimArgs = redis.XClaimArgs

// Z represents sorted set member.
type Z = redis.Z

// ZWithKey represents sorted set member including the name of the key where it was popped.
type ZWithKey = redis.ZWithKey

// ZStore is used as an arg to ZInterStore and ZUnionStore.
type ZStore = redis.ZStore

type ZRangeBy = redis.ZRangeBy

// ScanIterator is used to incrementally iterate over a collection of elements.
// It's safe for concurrent use by multiple goroutines.
type ScanIterator = redis.ScanIterator

// Limiter is the interface of a rate limiter or a circuit breaker.
type Limiter = redis.Limiter

type Options = redis.Options

// ParseURL parses an URL into Options that can be used to connect to Redis.
func ParseURL(redisURL string) (*Options, error) {
	return redis.ParseURL(redisURL)
}

// Pipeliner is an mechanism to realise Redis Pipeline technique.
//
// Pipelining is a technique to extremely speed up processing by packing
// operations to batches, send them at once to Redis and read a replies in a
// singe step.
// See https://redis.io/topics/pipelining
//
// Pay attention, that Pipeline is not a transaction, so you can get unexpected
// results in case of big pipelines and small read/write timeouts.
// Redis client has retransmission logic in case of timeouts, pipeline
// can be retransmitted and commands can be executed more then once.
// To avoid this: it is good idea to use reasonable bigger read/write timeouts
// depends of your batch size and/or use TxPipeline.
type Pipeliner = redis.Pipeliner

// Pipeline implements pipelining as described in
// http://redis.io/topics/pipelining. It's safe for concurrent use
// by multiple goroutines.
type Pipeline = redis.Pipeline

// PubSub implements Pub/Sub commands as described in
// http://redis.io/topics/pubsub. Message receiving is NOT safe
// for concurrent use by multiple goroutines.
//
// PubSub automatically reconnects to Redis Server and resubscribes
// to the channels in case of network errors.
type PubSub = redis.PubSub

// Subscription received after a successful subscription to channel.
type Subscription = redis.Subscription

// Message received as result of a PUBLISH command issued by another client.
type Message = redis.Message

// Pong received as result of a PING command issued by another client.
type Pong = redis.Pong

// Nil reply returned by Redis when key does not exist.
const Nil = redis.Nil

func SetLogger(logger *log.Logger) {
	redis.SetLogger(logger)
}

type Hook = redis.Hook

// Client is a Redis client representing a pool of zero or more
// underlying connections. It's safe for concurrent use by multiple
// goroutines.
type Client = redis.Client

// NewClient returns a client to the Redis Server specified by Options.
func NewClient(opt *Options) *Client {
	return redis.NewClient(opt)
}

type PoolStats = redis.PoolStats

// Conn is like Client, but its pool contains single connection.
type Conn = redis.Conn

// NewCmdResult returns a Cmd initialised with val and err for testing
func NewCmdResult(val interface{}, err error) *Cmd {
	return redis.NewCmdResult(val, err)
}

// NewSliceResult returns a SliceCmd initialised with val and err for testing
func NewSliceResult(val []interface{}, err error) *SliceCmd {
	return redis.NewSliceResult(val, err)
}

// NewStatusResult returns a StatusCmd initialised with val and err for testing
func NewStatusResult(val string, err error) *StatusCmd {
	return redis.NewStatusResult(val, err)
}

// NewIntResult returns an IntCmd initialised with val and err for testing
func NewIntResult(val int64, err error) *IntCmd {
	return redis.NewIntResult(val, err)
}

// NewDurationResult returns a DurationCmd initialised with val and err for testing
func NewDurationResult(val time.Duration, err error) *DurationCmd {
	return redis.NewDurationResult(val, err)
}

// NewBoolResult returns a BoolCmd initialised with val and err for testing
func NewBoolResult(val bool, err error) *BoolCmd {
	return redis.NewBoolResult(val, err)
}

// NewStringResult returns a StringCmd initialised with val and err for testing
func NewStringResult(val string, err error) *StringCmd {
	return redis.NewStringResult(val, err)
}

// NewFloatResult returns a FloatCmd initialised with val and err for testing
func NewFloatResult(val float64, err error) *FloatCmd {
	return redis.NewFloatResult(val, err)
}

// NewStringSliceResult returns a StringSliceCmd initialised with val and err for testing
func NewStringSliceResult(val []string, err error) *StringSliceCmd {
	return redis.NewStringSliceResult(val, err)
}

// NewBoolSliceResult returns a BoolSliceCmd initialised with val and err for testing
func NewBoolSliceResult(val []bool, err error) *BoolSliceCmd {
	return redis.NewBoolSliceResult(val, err)
}

// NewStringStringMapResult returns a StringStringMapCmd initialised with val and err for testing
func NewStringStringMapResult(val map[string]string, err error) *StringStringMapCmd {
	return redis.NewStringStringMapResult(val, err)
}

// NewStringIntMapCmdResult returns a StringIntMapCmd initialised with val and err for testing
func NewStringIntMapCmdResult(val map[string]int64, err error) *StringIntMapCmd {
	return redis.NewStringIntMapCmdResult(val, err)
}

// NewZSliceCmdResult returns a ZSliceCmd initialised with val and err for testing
func NewZSliceCmdResult(val []Z, err error) *ZSliceCmd {
	return redis.NewZSliceCmdResult(val, err)
}

// NewZWithKeyCmdResult returns a NewZWithKeyCmd initialised with val and err for testing
func NewZWithKeyCmdResult(val *ZWithKey, err error) *ZWithKeyCmd {
	return redis.NewZWithKeyCmdResult(val, err)
}

// NewScanCmdResult returns a ScanCmd initialised with val and err for testing
func NewScanCmdResult(keys []string, cursor uint64, err error) *ScanCmd {
	return redis.NewScanCmdResult(keys, cursor, err)
}

// NewClusterSlotsCmdResult returns a ClusterSlotsCmd initialised with val and err for testing
func NewClusterSlotsCmdResult(val []ClusterSlot, err error) *ClusterSlotsCmd {
	return redis.NewClusterSlotsCmdResult(val, err)
}

// NewGeoLocationCmdResult returns a GeoLocationCmd initialised with val and err for testing
func NewGeoLocationCmdResult(val []GeoLocation, err error) *GeoLocationCmd {
	return redis.NewGeoLocationCmdResult(val, err)
}

// NewCommandsInfoCmdResult returns a CommandsInfoCmd initialised with val and err for testing
func NewCommandsInfoCmdResult(val map[string]*CommandInfo, err error) *CommandsInfoCmd {
	return redis.NewCommandsInfoCmdResult(val, err)
}

// Hash is type of hash function used in consistent hash.
type Hash = redis.Hash

// RingOptions are used to configure a ring client and should be
// passed to NewRing.
type RingOptions = redis.RingOptions

// Ring is a Redis client that uses consistent hashing to distribute
// keys across multiple Redis servers (shards). It's safe for
// concurrent use by multiple goroutines.
//
// Ring monitors the state of each shard and removes dead shards from
// the ring. When a shard comes online it is added back to the ring. This
// gives you maximum availability and partition tolerance, but no
// consistency between different shards or even clients. Each client
// uses shards that are available to the client and does not do any
// coordination when shard state is changed.
//
// Ring should be used when you need multiple Redis servers for caching
// and can tolerate losing data when one of the servers dies.
// Otherwise you should use Redis Cluster.
type Ring = redis.Ring

func NewRing(opt *RingOptions) *Ring {
	return redis.NewRing(opt)
}

type Script = redis.Script

func NewScript(src string) *Script {
	return redis.NewScript(src)
}

// FailoverOptions are used to configure a failover client and should
// be passed to NewFailoverClient.
type FailoverOptions = redis.FailoverOptions

// NewFailoverClient returns a Redis client that uses Redis Sentinel
// for automatic failover. It's safe for concurrent use by multiple
// goroutines.
func NewFailoverClient(failoverOpt *FailoverOptions) *Client {
	return redis.NewFailoverClient(failoverOpt)
}

type SentinelClient = redis.SentinelClient

func NewSentinelClient(opt *Options) *SentinelClient {
	return redis.NewSentinelClient(opt)
}

// TxFailedErr transaction redis failed.
const TxFailedErr = redis.TxFailedErr

// Tx implements Redis transactions as described in
// http://redis.io/topics/transactions. It's NOT safe for concurrent use
// by multiple goroutines, because Exec resets list of watched keys.
// If you don't need WATCH it is better to use Pipeline.
type Tx = redis.Tx

// UniversalOptions information is required by UniversalClient to establish
// connections.
type UniversalOptions = redis.UniversalOptions

// UniversalClient is an abstract client which - based on the provided options -
// can connect to either clusters, or sentinel-backed failover instances
// or simple single-instance servers. This can be useful for testing
// cluster-specific applications locally.
type UniversalClient = redis.UniversalClient

// NewUniversalClient returns a new multi client. The type of client returned depends
// on the following three conditions:
//
// 1. if a MasterName is passed a sentinel-backed FailoverClient will be returned
// 2. if the number of Addrs is two or more, a ClusterClient will be returned
// 3. otherwise, a single-node redis Client will be returned.
func NewUniversalClient(opts *UniversalOptions) UniversalClient {
	return redis.NewUniversalClient(opts)
}
