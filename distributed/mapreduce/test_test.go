package mapreduce

import (
	"bufio"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	nNumber = 100000
	nMap    = 100
	nReduce = 50
)

// Deletes file at path
func testRemoveFile(path string) {
	if err := os.Remove(path); err != nil {
		log.Fatal("test.go testRemoveFile: ", err)
	}
}

// Create input file with N numbers
// Check if we have N numbers in output file

// Split in words
func MapFunc(value string) *list.List {
	DPrintf("Map %v\n", value)
	res := list.New()
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res.PushBack(kv)
	}
	return res
}

// Just return key
func ReduceFunc(key string, values *list.List) string {
	for e := values.Front(); e != nil; e = e.Next() {
		DPrintf("Reduce %s %v\n", key, e.Value)
	}
	return ""
}

// Checks input file against output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, file string, outputFile string) {
	input, err := os.Open(file)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer input.Close()
	output, err := os.Open(outputFile)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	inputScanner := bufio.NewScanner(input)
	for inputScanner.Scan() {
		lines = append(lines, inputScanner.Text())
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i += 1
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInput() string {
	name := "134-mrinput.txt"
	file, err := os.Create(name)
	if err != nil {
		log.Fatal("mkInput: ", err)
	}
	w := bufio.NewWriter(file)
	for i := 0; i < nNumber; i++ {
		fmt.Fprintf(w, "%d\n", i)
	}
	w.Flush()
	file.Close()
	return name
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp.
func port(suffix string) string {
	s := "/var/tmp/134-mapreduce-" + strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *MapReduce {
	file := makeInput()
	output := "mrtmp." + file
	master := port("master")
	mr := MakeMapReduce(nMap, nReduce, file, output, master)
	return mr
}

func cleanup(mr *MapReduce) {
	mr.CleanupFiles()
	testRemoveFile(mr.file)
}

func localRemoveFileIfExists(path string) {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal("localRemoveFileIfExists: ", err)
	}
}

func localReadFilePairs(path string) []KeyValue {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("localReadFilePairs: ", err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	var pairs []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatal("localReadFilePairs: ", err)
		}
		pairs = append(pairs, kv)
	}

	return pairs
}

func localOpenPairsFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("localOpenPairsFile: ", err)
	}
	return file
}

func localAppendPairsFilePair(file *os.File, pair KeyValue) {
	if err := json.NewEncoder(file).Encode(&pair); err != nil {
		log.Fatal("localAppendPairsFilePair: ", err)
	}
}

func localClosePairsFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Fatal("localClosePairsFile: ", err)
	}
}

func localSortedPairs(pairs []KeyValue) []KeyValue {
	out := append([]KeyValue(nil), pairs...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key == out[j].Key {
			return out[i].Value < out[j].Value
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func TestPartIIA_DoMapPartitioning(t *testing.T) {
	file := fmt.Sprintf("iia-domap-%d", time.Now().UnixNano())
	mapJob := 0
	nReduceLocal := 3
	inputSplit := "mrtmp." + file + "-0.tmp"
	partition0 := inputSplit + "-0.tmp"
	partition1 := inputSplit + "-1.tmp"
	partition2 := inputSplit + "-2.tmp"
	partition3 := inputSplit + "-3.tmp"

	localRemoveFileIfExists(file)

	if err := os.WriteFile(inputSplit, []byte("apple banana apple carrot"), 0644); err != nil {
		t.Fatalf("failed to create map input split: %v", err)
	}

	defer localRemoveFileIfExists(inputSplit)
	defer localRemoveFileIfExists(partition0)
	defer localRemoveFileIfExists(partition1)
	defer localRemoveFileIfExists(partition2)

	mapWords := func(value string) *list.List {
		res := list.New()
		res.PushBack(KeyValue{Key: "1", Value: value})
		res.PushBack(KeyValue{Key: "1", Value: value})
		res.PushBack(KeyValue{Key: "2", Value: value})
		res.PushBack(KeyValue{Key: "3", Value: value})
		res.PushBack(KeyValue{Key: "1", Value: "1"})
		res.PushBack(KeyValue{Key: "7", Value: "1"})
		return res
	}

	DoMap(mapJob, file, nReduceLocal, mapWords)

	got0 := localReadFilePairs(partition0)
	got1 := localReadFilePairs(partition1)
	got2 := localReadFilePairs(partition2)

	want0 := []KeyValue{{Key: "7", Value: "1"}}
	want1 := []KeyValue{{Key:"1",Value:"apple banana apple carrot"}, {Key:"1",Value:"apple banana apple carrot"}, {Key:"2",Value:"apple banana apple carrot"}, {Key:"1",Value:"1"}}
	want2 := []KeyValue{{Key: "3", Value: "apple banana apple carrot"}}

	if !equalLocalPairsIgnoreOrder(got0, want0) {
		t.Fatalf("partition 0 mismatch: got %+v, wanted %+v", got0, want0)
	}
	if !equalLocalPairsIgnoreOrder(got1, want1) {
		t.Fatalf("partition 1 mismatch: got %+v, wanted %+v", got1, want1)
	}
	if !equalLocalPairsIgnoreOrder(got2, want2) {
		t.Fatalf("partition 2 content mismatch: got %+v, wanted %+v", got2, want2)
	}
	// check if partition 3 file exists, and if so, fail
	if _, err := os.Stat(partition3); err == nil {
		t.Fatalf("unexpected partition file %s was created", partition3)
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Fatal("TestPartIIA_DoMapPartitioning: unexpected error checking for partition3: ", err)
	}
}

func equalLocalPairsIgnoreOrder(a []KeyValue, b []KeyValue) bool {
	if len(a) != len(b) {
		return false
	}
	aSorted := localSortedPairs(a)
	bSorted := localSortedPairs(b)
	for i := range aSorted {
		if aSorted[i] != bSorted[i] {
			return false
		}
	}
	return true
}

func TestPartIIA_DoReduceAggregatesAndSorts(t *testing.T) {
	file := fmt.Sprintf("iia-doreduce-%d", time.Now().UnixNano())
	reduceJob := 1
	nMapLocal := 2
	input0 := "mrtmp." + file + "-0.tmp-1.tmp"
	input1 := "mrtmp." + file + "-1.tmp-1.tmp"
	decoy0 := "mrtmp." + file + "-0.tmp-0.tmp"
	decoy1 := "mrtmp." + file + "-1.tmp-0.tmp"
	output := "mrtmp." + file + "-res-1.tmp"

	defer localRemoveFileIfExists(input0)
	defer localRemoveFileIfExists(input1)
	defer localRemoveFileIfExists(decoy0)
	defer localRemoveFileIfExists(decoy1)
	defer localRemoveFileIfExists(output)
	localRemoveFileIfExists(input0)
	localRemoveFileIfExists(input1)
	localRemoveFileIfExists(decoy0)
	localRemoveFileIfExists(decoy1)
	localRemoveFileIfExists(output)

	map0 := localOpenPairsFile(input0)
	localAppendPairsFilePair(map0, KeyValue{Key: "banana", Value: "1"})
	localAppendPairsFilePair(map0, KeyValue{Key: "apple", Value: "5"})
	localClosePairsFile(map0)

	map1 := localOpenPairsFile(input1)
	localAppendPairsFilePair(map1, KeyValue{Key: "apple", Value: "1"})
	localAppendPairsFilePair(map1, KeyValue{Key: "cherry", Value: "1"})
	localClosePairsFile(map1)

	decoyMap0 := localOpenPairsFile(decoy0)
	localAppendPairsFilePair(decoyMap0, KeyValue{Key: "zzz_decoy", Value: "999"})
	localClosePairsFile(decoyMap0)
	decoyMap1 := localOpenPairsFile(decoy1)
	localAppendPairsFilePair(decoyMap1, KeyValue{Key: "yyy_decoy", Value: "888"})
	localClosePairsFile(decoyMap1)

	reduceCount := func(key string, values *list.List) string {
		s := ""
		for e := values.Front(); e != nil; e = e.Next() {
			s = s + fmt.Sprintf("%v-", e.Value)
		}
		return s
	}

	DoReduce(reduceJob, file, nMapLocal, reduceCount)

	got := localReadFilePairs(output)
	want1 := []KeyValue{
		{Key: "apple", Value: "1-5-"},
		{Key: "banana", Value: "1-"},
		{Key: "cherry", Value: "1-"},
	}
	want2 := []KeyValue{
		{Key: "apple", Value: "5-1-"},
		{Key: "banana", Value: "1-"},
		{Key: "cherry", Value: "1-"},
	}

	if len(got) != len(want1) {
		t.Fatalf("DoReduce wrote wrong number of output pairs: want %d, got %d", len(want1), len(got))
	}
	// Check if got matches want1 or want2
	if !equalLocalPairsIgnoreOrder(got, want1) && !equalLocalPairsIgnoreOrder(got, want2) {
		t.Fatalf("DoReduce output mismatch: got %+v, wanted %+v or %+v", got, want1, want2)
	}
}

func TestPartIIB_Basic(t *testing.T) {
	fmt.Printf("Test: Basic mapreduce ...\n")
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file, mr.outputFile)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... Basic Passed\n")
}

func TestPartIIB_Parallelism(t *testing.T) {
	fmt.Printf("Test: Parallelism ...\n")
	mr := setup()

	var maxConcurrent int32
	var currentConcurrent int32

	parallelMapFunc := func(value string) *list.List {
		n := atomic.AddInt32(&currentConcurrent, 1)
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if n <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&currentConcurrent, -1)
		return MapFunc(value)
	}

	for i := 0; i < 2; i++ {
		go RunWorker(mr.MasterAddress, port("pworker"+strconv.Itoa(i)),
			parallelMapFunc, ReduceFunc, -1)
	}

	<-mr.DoneChannel

	mc := atomic.LoadInt32(&maxConcurrent)
	if mc < 2 {
		t.Fatalf("Expected at least 2 concurrent map jobs, but max was %d. "+
			"Ensure your master dispatches jobs to workers in parallel.", mc)
	}

	check(t, mr.file, mr.outputFile)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... Parallelism Passed\n")
}

func TestPartIII_OneFailure(t *testing.T) {
	fmt.Printf("Test: One Failure mapreduce ...\n")
	mr := setup()
	// Start 2 workers that fail after 10 jobs
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10)
	go RunWorker(mr.MasterAddress, port("worker"+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1)
	// Wait until MR is done
	<-mr.DoneChannel
	check(t, mr.file, mr.outputFile)
	checkWorker(t, mr.stats)
	cleanup(mr)
	fmt.Printf("  ... One Failure Passed\n")
}

func TestPartIII_ManyFailures(t *testing.T) {
	fmt.Printf("Test: ManyFailures mapreduce ...\n")
	mr := setup()
	i := 0
	done := false
Loop:
	for !done {
		select {
		case done = <-mr.DoneChannel:
			check(t, mr.file, mr.outputFile)
			cleanup(mr)
			break Loop
		default:
			// Start 2 workers each sec. The workers fail after 10 jobs
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, MapFunc, ReduceFunc, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.MasterAddress, w, MapFunc, ReduceFunc, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}

	fmt.Printf("  ... Many Failures Passed\n")
}

func TestPartIII_Timeout(t *testing.T) {
	fmt.Printf("Test: Timeout ...\n")
	mr := setup()

	// Start one worker that dies after 5 RPCs
	go RunWorker(mr.MasterAddress, port("timeout-worker0"),
		MapFunc, ReduceFunc, 5)

	// After a short delay, start a reliable backup worker
	backupWorkerStarted := make(chan struct{})
	go func() {
		time.Sleep(3 * time.Second)
		close(backupWorkerStarted)
		go RunWorker(mr.MasterAddress, port("timeout-worker1"),
			MapFunc, ReduceFunc, -1)
	}()

	done := make(chan bool, 1)
	go func() {
		<-mr.DoneChannel
		done <- true
	}()

	select {
	case <-done:
		select {
		case <-backupWorkerStarted:
		default:
			t.Fatalf("MapReduce completed before backup worker started")
		}
		check(t, mr.file, mr.outputFile)
		checkWorker(t, mr.stats)
		cleanup(mr)
	case <-time.After(10 * time.Second):
		t.Fatalf("MapReduce did not complete within 10 seconds -- " +
			"does your master handle worker timeouts?")
	}

	fmt.Printf("  ... Timeout Passed\n")
}
