package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type Member string

func (m Member) String() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// type Indspread struct {
// 	x  int
// 	y  int
// 	z  float32
// 	sp int
// }

var wg sync.WaitGroup

func main() {
	var y = []int{2, 10, 2}
	var z = []float64{1.2, 2.0, 0.2}

	var chSize int
	ysiz := int(math.Ceil(float64((y[1]-y[0])/y[2]))) + 1
	zsiz := int(math.Ceil(float64((z[1]-z[0])/z[2]))) + 1

	csvFile, err := os.Create("results.csv")

	if err != nil {
		fmt.Println("failed creating file: %s", err)
	}
	csvwriter := csv.NewWriter(csvFile)

	x := findNextNPrimes(200, 4)

	chSize = ysiz * zsiz * len(x) * 2
	ch := make(chan []string, chSize)

	for _, i := range x {
		for j := y[0]; j <= y[1]; j += y[2] {
			for k := z[0]; k <= z[1]; k += z[2] {
				wg.Add(2)
				go calculateSpread(i, j, k, 90, ch)
				go calculateSpread(i, j, k, 120, ch)
				// go calculateSpread(i, j, k, 150000, ch)
				// go calculateSpread(i, j, k, 200000, ch)
			}
		}
	}

	wg.Wait()
	close(ch)

	for val := range ch {

		_ = csvwriter.Write(val)
	}

	csvwriter.Flush()
	csvFile.Close()

}

func findNextNPrimes(start, n int) []int {
	primes := []int{}

	for len(primes) < n {
		if isPrime(start) {
			primes = append(primes, start)
		}
		start++
	}

	return primes
}

func isPrime(n int) bool {
	if n <= 1 {
		return false
	}

	if n <= 3 {
		return true
	}

	if (n%2 == 0) || (n%3 == 0) {
		return false
	}

	for i := 5; i < (int(math.Sqrt(float64(n)) + 1)); i += 6 {

		if n%i == 0 || n%(i+2) == 0 {
			return false
		}
	}
	return true
}

func calculateSpread(x int, y int, z float64, kc int, ch chan []string) {
	members := []consistent.Member{}
	for i := 0; i < 8; i++ {
		member := Member(fmt.Sprintf("node%d.olricmq", i))
		members = append(members, member)
	}
	cfg := consistent.Config{
		PartitionCount:    x,
		ReplicationFactor: y,
		Load:              z,
		Hasher:            hasher{},
	}
	c := consistent.New(members, cfg)

	keyCount := kc
	load := (c.AverageLoad() * float64(keyCount)) / float64(cfg.PartitionCount)
	fmt.Println("Maximum key count for a member should be around this: ", math.Ceil(load))
	distribution := make(map[string]int)
	key := make([]byte, 4)
	for i := 0; i < keyCount; i++ {
		rand.Read(key)
		member := c.LocateKey(key)
		distribution[member.String()]++
	}
	// for member, count := range distribution {
	// 	fmt.Printf("member: %s, key count: %d\n", member, count)
	// }

	defer wg.Done()

	var max = float64(0)
	var min = math.Inf(0)

	fmt.Println("tthe max,min:", max, min)

	for _, count := range distribution {
		if float64(count) < min {
			min = float64(count)
		}

		if float64(count) > max {
			max = float64(count)
		}
	}

	fmt.Println("the max,min:", max, min)

	sp := []string{fmt.Sprintf("%d", x), fmt.Sprintf("%d", y), fmt.Sprintf("%f", z), fmt.Sprintf("%d", kc), fmt.Sprintf("%f", max-min)}

	fmt.Println("the sp:", sp)

	ch <- sp

}
