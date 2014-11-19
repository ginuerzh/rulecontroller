package main

import (
	//"encoding/json"
	"flag"
	"github.com/garyburd/redigo/redis"
	"github.com/shevilangle/rulecontroller/models"
	"log"
	"time"
)

var (
	redisServer string
	fromString  string
	toString    string
	pool        *redis.Pool
)

type publishData struct {
	RuleID int `json:"ruleid"`
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.StringVar(&fromString, "f", "sports:pubsub:notice", "listen channel")
	flag.StringVar(&toString, "t", "rulecontroller:notice", "receiver channel")
	flag.StringVar(&redisServer, "r", "172.24.222.54:6379", "redis server")
	flag.StringVar(&models.MongoAddr, "m", "localhost:27017", "mongodb server")
	flag.Parse()
}

func main() {
	pool = getRedisPool()
	listenChannel()
	log.Println("over")
}

func getRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisServer)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func listenChannel() {
	conn := pool.Get()
	defer conn.Close()
	psc := redis.PubSubConn{conn}
	psc.Subscribe(fromString)
	for {
		switch n := psc.Receive().(type) {
		case redis.Message:
			log.Println("Message,channel :", n.Channel, ", data :", n.Data)
			errs, result := models.RebuildPushData(n.Data)
			if errs == nil {
				notice(result)
			} else {
				log.Println("errs: ", errs)
				//panic(errs)
			}
		case redis.PMessage:
			log.Println("PMessage, pattern: ", n.Pattern, ", channel :", n.Channel, ", data :", n.Data)
		case redis.Subscription:
			log.Println("Subscription, kind: ", n.Kind, ", channel:", n.Channel, ", count:", n.Count)
			if n.Count == 0 {
				return
			}
		case error:
			log.Println("error: ", n)
			return
		}
	}
}

func notice(result []byte) {
	conn := pool.Get()
	defer conn.Close()

	/*
		pubData := &publishData{
			RuleID: ruleid,
		}
			pubData, err := json.Marshal(es[i])
			if err != nil {
				log.Println("error: ", err)
				return
			}
	*/
	//log.Println("pubData: ", pubData)
	_, err := conn.Do("PUBLISH", toString, result)
	log.Println("err: ", err)
}
