package benchmark

import (
	bitcask "Bitcask_go"
	"Bitcask_go/config"
	"Bitcask_go/util"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var db *bitcask.DB

func init() {
	cfg := config.DefaultOptions
	cfg.DataDir, _ = os.MkdirTemp("", "bitcask-go-benchmark")
	var err error
	db, err = bitcask.Open(cfg)

	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	//重置计时器
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(util.GetTestKey(i), util.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(util.GetTestKey(i), util.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(util.GetTestKey(rand.Int()))
		if err != nil && err != util.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(util.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
