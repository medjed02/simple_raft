package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	crypto "crypto/rand"
	"math/rand"

	"github.com/medjed02/simple_raft"
)

type Config struct {
	replicas map[int]simple_raft.ReplicaInfo
	id       int
	http     string
}

func getConfig() Config {
	cfg := Config{}
	cfg.replicas = make(map[int]simple_raft.ReplicaInfo)

	id := flag.Int("node", 1, "node id, integer")
	http := flag.String("http", ":5030", "node http addr, string")
	replicasString := flag.String("replicas", "", "replicas info in format $id1,$ip1;$id2,$ip2, string")
	flag.Parse()

	cfg.http = *http
	cfg.id = *id

	for _, part := range strings.Split(*replicasString, ";") {
		var replicaInfo simple_raft.ReplicaInfo

		idAddress := strings.Split(part, ",")
		var err error
		id, err := strconv.ParseUint(idAddress[0], 10, 64)
		if err != nil {
			panic("incorrect id in replicas string")
		}
		replicaInfo.Address = idAddress[1]
		replicaInfo.HttpAddress = strings.Split(replicaInfo.Address, ":")[0] + ":" + strconv.Itoa(int(5029+id))
		cfg.replicas[int(id)] = replicaInfo
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
	s.StateMachine.stateLock.Lock()
	defer s.StateMachine.stateLock.Unlock()

	key := r.URL.Query().Get("key")
	commitIndex := r.URL.Query().Get("commited")

	var value string
	var err error

	replica := s.Consensus.GetGoodReplica()
	myCommitIndex := strconv.Itoa(s.Consensus.GetCommitIndex())
	if replica != "" {
		w.Header().Add("CommitIndex", myCommitIndex)
		http.Redirect(w, r, replica, http.StatusFound)
		return
	} else {
		if len(commitIndex) != 0 && myCommitIndex != commitIndex {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
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

	s := simple_raft.NewServer(cfg.replicas, stateMachine, cfg.id)
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
