package redis_test

import (
	"math/rand"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/redis.v2"
)

var _ = Describe("Cluster", func() {
	var scenario = struct {
		ports     []string
		nodeIDs   []string
		processes map[string]*redisProcess
		clients   map[string]*redis.Client
		master    *redis.Client
	}{
		ports:     []string{"8220", "8221", "8222", "8223", "8224", "8225"},
		nodeIDs:   make([]string, 6),
		processes: make(map[string]*redisProcess, 6),
		clients:   make(map[string]*redis.Client, 6),
	}

	BeforeSuite(func() {
		// Start processes, connect individual clients
		for pos, port := range scenario.ports {
			process, err := startRedis(port, "--cluster-enabled", "yes")
			Expect(err).NotTo(HaveOccurred())

			client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port})
			info, err := client.ClusterNodes().Result()
			Expect(err).NotTo(HaveOccurred())

			scenario.processes[port] = process
			scenario.clients[port] = client
			scenario.nodeIDs[pos] = info[:40]
		}
		scenario.master = scenario.clients[scenario.ports[0]]

		// Meet cluster nodes
		for _, port := range scenario.ports {
			client := scenario.clients[port]
			err := client.ClusterMeet("127.0.0.1", scenario.ports[0]).Err()
			Expect(err).NotTo(HaveOccurred())
		}

		// Bootstrap masters
		slots := []int{0, 5000, 10000, 16384}
		for pos, port := range scenario.ports[:3] {
			client := scenario.clients[port]
			err := client.ClusterAddSlotsRange(slots[pos], slots[pos+1]-1).Err()
			Expect(err).NotTo(HaveOccurred())
		}

		// Bootstrap slaves
		for pos, port := range scenario.ports[3:] {
			client := scenario.clients[port]
			masterID := scenario.nodeIDs[pos]

			Eventually(func() string { // Wait for masters
				return client.ClusterNodes().Val()
			}, "10s").Should(ContainSubstring(masterID))

			err := client.ClusterReplicate(masterID).Err()
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() string { // Wait for slaves
				return scenario.master.ClusterNodes().Val()
			}, "10s").Should(ContainSubstring("slave " + masterID))
		}

		Eventually(func() string { // Wait for cluster state to turn OK
			return scenario.master.ClusterInfo().Val()
		}, "10s").Should(ContainSubstring("cluster_state:ok"))
	})

	AfterSuite(func() {
		for _, client := range scenario.clients {
			client.Close()
		}
		for _, process := range scenario.processes {
			process.Close()
		}
	})

	Describe("HashSlot", func() {

		It("should calculate hash slots", func() {
			tests := []struct {
				key  string
				slot int
			}{
				{"123456789", 12739},
				{"{}foo", 9500},
				{"foo{}", 5542},
				{"foo{}{bar}", 8363},
				{"", 10503},
				{"", 5176},
				{string([]byte{83, 153, 134, 118, 229, 214, 244, 75, 140, 37, 215, 215}), 5463},
			}
			rand.Seed(100)

			for _, test := range tests {
				Expect(redis.HashSlot(test.key)).To(Equal(test.slot), "for %s", test.key)
			}
		})

		It("should extract keys from tags", func() {
			tests := []struct {
				one, two string
			}{
				{"foo{bar}", "bar"},
				{"{foo}bar", "foo"},
				{"{user1000}.following", "{user1000}.followers"},
				{"foo{{bar}}zap", "{bar"},
				{"foo{bar}{zap}", "bar"},
			}

			for _, test := range tests {
				Expect(redis.HashSlot(test.one)).To(Equal(redis.HashSlot(test.two)), "for %s <-> %s", test.one, test.two)
			}
		})

	})

	Describe("Commands", func() {

		It("should CLUSTER SLOTS", func() {
			res, err := scenario.master.ClusterSlots().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))
			Expect(res).To(ConsistOf([]redis.ClusterSlotInfo{
				{0, 4999, []string{"127.0.0.1:8220", "127.0.0.1:8223"}},
				{5000, 9999, []string{"127.0.0.1:8221", "127.0.0.1:8224"}},
				{10000, 16383, []string{"127.0.0.1:8222", "127.0.0.1:8225"}},
			}))
		})

		It("should CLUSTER NODES", func() {
			res, err := scenario.master.ClusterNodes().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res)).To(BeNumerically(">", 400))
		})

		It("should CLUSTER INFO", func() {
			res, err := scenario.master.ClusterInfo().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainSubstring("cluster_known_nodes:6"))
		})

	})

	Describe("Client", func() {
		var client *redis.ClusterClient

		BeforeEach(func() {
			var err error
			client, err = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{"127.0.0.1:8220", "127.0.0.1:8221", "127.0.0.1:8222", "127.0.0.1:8223", "127.0.0.1:8224", "127.0.0.1:8225"},
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			for _, node := range scenario.clients {
				node.FlushDb()
			}
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("should retrieve master address for slot", func() {
			addr := client.GetMasterAddrBySlot(4000)
			Expect(addr).To(ContainSubstring("127.0.0.1:"))
		})

		It("should retrieve slave addresses for slot", func() {
			addrs := client.GetSlaveAddrsBySlot(4000)
			Expect(addrs).To(HaveLen(1))
		})

		It("should GET/SET/DEL", func() {
			val, err := client.Get("A").Result()
			Expect(err).To(Equal(redis.Nil))
			Expect(val).To(Equal(""))

			val, err = client.Set("A", "VALUE").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))

			val, err = client.Get("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("VALUE"))

			cnt, err := client.Del("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(Equal(int64(1)))
		})

		It("should follow redirects", func() {
			slot := redis.HashSlot("A")
			Expect(client.Set("A", "VALUE").Err()).NotTo(HaveOccurred())

			addrs := client.GetSlaveAddrsBySlot(slot)
			Expect(addrs).To(HaveLen(1))

			val, err := client.GetNodeClientByAddr(addrs[0]).ClusterFailover().Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("OK"))

			val, err = client.Get("A").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("VALUE"))
		})

	})
})
