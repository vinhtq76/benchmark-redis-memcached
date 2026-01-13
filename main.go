package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
)

func benchMemcachedSet() {
	mc := memcache.New("localhost:11211")

	var wg sync.WaitGroup
	const numThreads = 4
	wg.Add(numThreads)

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()
			for i := startIndex; i < endIndex; i++ {
				err := mc.Set(&memcache.Item{
					Key:   fmt.Sprintf("KEY%07d", i+1),
					Value: []byte(fmt.Sprintf("VALUE:%07d", i+1)),
				})
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()

	fmt.Println("Duration for Memcached SET 100,000, 4 threads:", time.Since(start))
}

func benchRedisGetBatch() float64 {
	client := redis.NewClient(&redis.Options{
		MinIdleConns: 12,
	})

	var wg sync.WaitGroup
	const numThreads = 12
	wg.Add(numThreads)

	fmt.Println("NUM Threads:", numThreads)

	const batchKeys = 80

	start := time.Now()
	for thread := 0; thread < numThreads; thread++ {
		const perThread = 100_000
		startIndex := thread * perThread
		endIndex := (thread + 1) * perThread
		go func() {
			defer wg.Done()

			total := 0
			for i := startIndex; i < endIndex; {
				keys := make([]string, 0, batchKeys)
				for k := 0; k < batchKeys; k++ {
					key := computeKey(i)
					keys = append(keys, key)
					i++
				}
				total += len(keys)
				// values := fmt.Sprintf("VALUE:%07d", i+1)
				_, err := client.MGet(context.Background(), keys...).Result()
				if err != nil {
					panic(err)
				}
			}
			fmt.Println("TOTAL:", total)
		}()
	}
	wg.Wait()

	d := time.Since(start)
	fmt.Printf("Duration for Redis GET 100,000, %d threads, batch %d: %v\n",
		numThreads, batchKeys, d)
	return d.Seconds() * 1000
}

func computeKey(input int) string {
	input = input + 1

	var data [256]byte
	result := data[:0]

	var numberData [30]byte
	number := numberData[:0]

	if input == 0 {
		number = append(number, '0')
	}

	for input > 0 {
		number = append(number, '0'+byte(input%10))
		input = input / 10
	}

	result = append(result, "KEY"...)

	for i := 0; i < 7-len(number); i++ {
		result = append(result, '0')
	}

	for i := len(number) - 1; i >= 0; i-- {
		result = append(result, number[i])
	}

	return string(result)
}

func main() {
	benchMemcachedSet()

	sum := float64(0)
	const numLoops = 30

	for i := 0; i < numLoops; i++ {
		sum += benchRedisGetBatch()
	}

	avgAll := sum / float64(numLoops)
	fmt.Println("AVG ALL:", avgAll)
	fmt.Printf("AVG QPS: %.2f\n", 12*100_000.0*1000.0/avgAll)
}
