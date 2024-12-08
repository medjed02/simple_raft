package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	crypto "crypto/rand"
	"math/rand"

	"github.com/medjed02/simple_raft"
)

type Config struct {
	replicas map[int]simple_raft.ReplicaInfo
	index    int
	http     string
}

func getConfig() Config {
	cfg := Config{}
	cfg.replicas = make(map[int]simple_raft.ReplicaInfo)
	var node string
	for i, arg := range os.Args[1:] {
		if arg == "--node" {
			var err error
			node = os.Args[i+2]
			cfg.index, err = strconv.Atoi(node)
			if err != nil {
				log.Fatal("Expected $value to be a valid integer in `--node $value`, got: %s", node)
			}
			i++
			continue
		}

		if arg == "--http" {
			cfg.http = os.Args[i+2]
			i++
			continue
		}

		if arg == "--cluster" {
			cluster := os.Args[i+2]
			var clusterEntry simple_raft.ReplicaInfo
			for _, part := range strings.Split(cluster, ";") {
				idAddress := strings.Split(part, ",")
				var err error
				id, err := strconv.ParseUint(idAddress[0], 10, 64)
				if err != nil {
					log.Fatal("Expected $id to be a valid integer in `--cluster $id,$ip`, got: %s", idAddress[0])
				}
				clusterEntry.Address = idAddress[1]
				cfg.replicas[int(id)] = clusterEntry
			}

			i++
			continue
		}
	}

	if node == "" {
		log.Fatal("Missing required parameter: --node $index")
	}

	if cfg.http == "" {
		log.Fatal("Missing required parameter: --http $address")
	}

	if len(cfg.replicas) == 0 {
		log.Fatal("Missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	return cfg
}

const (
	Get            = iota
	Set            = iota
	Delete         = iota
	CompareAndSwap = iota
)

type Command struct {
	Kind     int
	Key      string
	OldValue string
	NewValue string
}

type StateMachine struct {
	db        map[string]string
	stateLock sync.Mutex
}

func encodeCommand(c Command) string {
	return fmt.Sprintf("%d#%s#%s#%s", c.Kind, c.Key, c.OldValue, c.NewValue)
}

func decodeCommand(s string) Command {
	words := strings.Split(s, "#")
	kind, err := strconv.Atoi(words[0])
	if err != nil {
		panic(err)
	}

	c := Command{
		Kind:     kind,
		Key:      words[1],
		OldValue: words[2],
		NewValue: words[3],
	}

	return c
}

func (m *StateMachine) Apply(s string) error {
	m.stateLock.Lock()
	defer m.stateLock.Unlock()

	c := decodeCommand(s)

	fmt.Println("apply command", s)

	switch c.Kind {
	case Set:
		m.db[c.Key] = c.NewValue
	case Delete:
		delete(m.db, c.Key)
	case CompareAndSwap:
		if value, ok := m.db[c.Key]; ok && value == c.OldValue {
			fmt.Println("swapped")
			m.db[c.Key] = c.NewValue
		}
	default:
		return fmt.Errorf("Unknown command: %x", c)
	}

	return nil
}

type KeyValueStorage struct {
	Consensus    *simple_raft.ConsensusServer
	StateMachine *StateMachine
}

func (s *KeyValueStorage) setHandler(w http.ResponseWriter, r *http.Request) {
	c := Command{
		Kind:     Set,
		Key:      r.URL.Query().Get("key"),
		OldValue: "",
		NewValue: r.URL.Query().Get("value"),
	}

	if !s.Consensus.ApplyCommand(encodeCommand(c)) {
		http.Redirect(w, r, s.Consensus.GetLeader(), http.StatusFound)
		return
	}
}

func (s *KeyValueStorage) deleteHandler(w http.ResponseWriter, r *http.Request) {
	c := Command{
		Kind:     Delete,
		Key:      r.URL.Query().Get("key"),
		OldValue: "",
		NewValue: "",
	}

	if !s.Consensus.ApplyCommand(encodeCommand(c)) {
		http.Redirect(w, r, s.Consensus.GetLeader(), http.StatusFound)
		return
	}
}

func (s *KeyValueStorage) casHandler(w http.ResponseWriter, r *http.Request) {
	c := Command{
		Kind:     CompareAndSwap,
		Key:      r.URL.Query().Get("key"),
		OldValue: r.URL.Query().Get("old"),
		NewValue: r.URL.Query().Get("new"),
	}

	if !s.Consensus.ApplyCommand(encodeCommand(c)) {
		http.Redirect(w, r, s.Consensus.GetLeader(), http.StatusFound)
		return
	}
}

func (s *KeyValueStorage) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	var value string
	var err error

	replica := s.Consensus.GetGoodReplica()
	if replica != "" {
		http.Redirect(w, r, replica, http.StatusFound)
		return
	} else {
		v, ok := s.StateMachine.db[key]
		if !ok {
			err = fmt.Errorf("Key not found")
		} else {
			value = v
		}
	}

	fmt.Println("getted value:", value)

	if err != nil {
		fmt.Println(err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	written := 0
	for written < len(value) {
		n, err := w.Write([]byte(value)[written:])
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		written += n
	}
}

func main() {
	var b [8]byte
	_, err := crypto.Read(b[:])
	if err != nil {
		panic("blin blinksii")
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))

	cfg := getConfig()

	db := make(map[string]string)
	stateMachine := &StateMachine{
		db:        db,
		stateLock: sync.Mutex{},
	}

	s := simple_raft.NewServer(cfg.replicas, stateMachine, cfg.index)
	go s.Launch()

	storage := KeyValueStorage{
		Consensus:    s,
		StateMachine: stateMachine,
	}

	http.HandleFunc("/set", storage.setHandler)
	http.HandleFunc("/delete", storage.deleteHandler)
	http.HandleFunc("/cas", storage.casHandler)
	http.HandleFunc("/get", storage.getHandler)
	err = http.ListenAndServe(cfg.http, nil)
	if err != nil {
		panic(err)
	}
}
