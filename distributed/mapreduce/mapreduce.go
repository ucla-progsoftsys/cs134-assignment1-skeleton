package mapreduce

import (
	"bufio"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

// import "os/exec"

// A simple mapreduce library with a sequential implementation.
//
// The application provides an input file f, a Map and Reduce function,
// and the number of nMap and nReduce tasks.
//
// Split() splits the file f in nMap input files, one for each Map job using the MapName() function for the output file names.
// Feel free to use MapName() in your own code to get the file names for each Map job.
//
// DoMap() runs Map on each map file, and DoReduce() runs Reduce on each reduce file.
// DoMap() and DoReduce() are called with the appropriate arguments when the master does call(workerAddress, "Worker.DoJob", ...),
// who tell the worker the job information it needs to execute.
//
// After all Map and Reduce jobs have finished, Merge() is called to merge the output into a single output
// Merge() expects there to be nReduce files named according to the MergeName() function string output.
// Feel free to use MergeName() to name the file used for output of your Reduce jobs
//
// Your job: implement everything that occurs after Split() and before Merge() is called.
//
// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Map and Reduce deal with <key, value> pairs:
type KeyValue struct {
	Key   string
	Value string
}

type MapReduce struct {
	nMap            int    // Number of Map jobs
	nReduce         int    // Number of Reduce jobs
	file            string // Name of input file
	outputFile      string // Name of output file
	MasterAddress   string
	registerChannel chan string
	DoneChannel     chan bool
	alive           atomic.Bool
	l               net.Listener
	stats           *list.List

	// Map of registered workers that you need to keep up to date
	// Make sure to lock the mutex before using the map
	// Example:
	//
	// mr.WorkersMutex.Lock()
	// ... do stuff with mr.Workers ...
	// mr.WorkersMutex.Unlock()

	Workers      map[string]*WorkerInfo
	WorkersMutex sync.Mutex

	// add any additional state here
}

func InitMapReduce(nmap int, nreduce int,
	file string, outputFile string, master string) *MapReduce {
	mr := new(MapReduce)
	mr.nMap = nmap
	mr.nReduce = nreduce
	mr.file = file
	mr.outputFile = outputFile
	mr.MasterAddress = master
	mr.alive.Store(true)
	mr.registerChannel = make(chan string)
	mr.DoneChannel = make(chan bool)

	// initialize any additional state here
	mr.Workers = make(map[string]*WorkerInfo)
	return mr
}

// ============== Server related code ==============
// You can ignore this section

func MakeMapReduce(nmap int, nreduce int,
	file string, outputFile string, master string) *MapReduce {
	mr := InitMapReduce(nmap, nreduce, file, outputFile, master)
	mr.StartRegistrationServer()
	go mr.Run()
	return mr
}

func (mr *MapReduce) Register(args *RegisterArgs, res *RegisterReply) error {
	DPrintf("Register: worker %s\n", args.Worker)
	mr.registerChannel <- args.Worker
	res.OK = true
	return nil
}

func (mr *MapReduce) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	DPrintf("Shutdown: registration server\n")
	mr.alive.Store(false)
	mr.l.Close() // causes the Accept to fail
	return nil
}

func (mr *MapReduce) StartRegistrationServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	removeFileIfExists(mr.MasterAddress) // only needed for "unix"
	l, e := net.Listen("unix", mr.MasterAddress)
	if e != nil {
		log.Fatal("RegistrationServer", mr.MasterAddress, " error: ", e)
	}
	mr.l = l

	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
		for mr.alive.Load() {
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				DPrintf("RegistrationServer: accept error %s", err)
				break
			}
		}
		DPrintf("RegistrationServer: done\n")
	}()
}

// ============== Helper Functions ==============
// These functions may be helpful to implement your code.
// Some are used by the existing code, so don't modify them.

// Use this to determine the file split
// A given map job's key-value output should be handled by Reduce task int(ihash(key string) % uint32(Number of Reduce tasks))
func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// ------------- File naming related functions -------------

// Name of the file that Split() creates for Map job <MapJob>
func MapName(fileName string, MapJob int) string {
	return "mrtmp." + fileName + "-" + strconv.Itoa(MapJob) + ".tmp"
}

// Name of the file that DoMap() creates for Reduce job <ReduceJob> created by Map job <MapJob>
func ReduceName(fileName string, MapJob int, ReduceJob int) string {
	return MapName(fileName, MapJob) + "-" + strconv.Itoa(ReduceJob) + ".tmp"
}

// Name of the files expected by Merge() to merge the results of reduce job <ReduceJob>
func MergeName(fileName string, ReduceJob int) string {
	return "mrtmp." + fileName + "-res-" + strconv.Itoa(ReduceJob) + ".tmp"
}

// ------------- File I/O related functions -------------

// Reads content of file at path as a single large string
func readFileString(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal("readFileString: ", err)
	}
	return string(b)
}

// (Over)writes content of file at path as a single large string
func writeFileString(path string, content string) {
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		log.Fatal("writeFileString: ", err)
	}
}

// Appends string content to file at path, creating file if it doesn't exist
func writeFileStringAppend(path string, content string) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("writeFileStringAppend: ", err)
	}
	defer file.Close()

	if _, err := file.WriteString(content); err != nil {
		log.Fatal("writeFileStringAppend: ", err)
	}
}

// Deletes file at path. Errors if file doesn't exist
func removeFile(path string) {
	if err := os.Remove(path); err != nil {
		log.Fatal("removeFile: ", err)
	}
}

// Deletes file at path if it exists.
func removeFileIfExists(path string) {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatal("removeFileIfExists: ", err)
	}
}

// Gets size of file at path
func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		log.Fatal("fileSize: ", err)
	}
	return fi.Size()
}

// ------------- KeyValue pair I/O related functions -------------
// Use these functions if you need to read/write KeyValue pairs to files

// Reads in pairs written to file using appendFilePair() and returns them as a slice of KeyValue pairs
// Hint: use this to pass data between the Map and Reduce phases!
func readFilePairs(path string) []KeyValue {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("readFilePairs: ", err)
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
			log.Fatal("readFilePairs: ", err)
		}
		pairs = append(pairs, kv)
	}

	return pairs
}

// Opens a file for appending KeyValue pairs, creating the file if it doesn't exist.
// The caller is responsible for closing the file with closePairsFile() when done.
func openPairsFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("openPairsFile: ", err)
	}
	return file
}

// Appends a KeyValue pair to an already-open pairs file
func appendPairsFilePair(file *os.File, pair KeyValue) {
	if err := json.NewEncoder(file).Encode(&pair); err != nil {
		log.Fatal("appendPairsFilePair: ", err)
	}
}

// Closes a pairs file opened with openPairsFile()
func closePairsFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.Fatal("closePairsFile: ", err)
	}
}

// Example usage of the above functions to write a slice of KeyValue pairs to a file:

// 	file := openPairsFile(path)
// 	defer closePairsFile(file)

// 	for _, pair := range pairs {
// 		appendPairsFilePair(file, pair)
// 	}

// ============== MapReduce logic related functions ==============

// Split bytes of input file into nMap splits, but split only on line boundaries
// You do not need to modify this function, as the code has already been provided
func (mr *MapReduce) Split(fileName string) {
	DPrintf("Split %s\n", fileName)
	infile, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Split: ", err)
	}
	defer infile.Close()
	size := fileSize(fileName)
	nchunk := size / int64(mr.nMap)
	nchunk += 1

	outfile, err := os.Create(MapName(fileName, 0))
	if err != nil {
		log.Fatal("Split: ", err)
	}
	writer := bufio.NewWriter(outfile)
	m := 1
	i := 0

	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		if int64(i) > nchunk*int64(m) {
			writer.Flush()
			outfile.Close()
			outfile, err = os.Create(MapName(fileName, m))
			if err != nil {
				log.Fatal("Split: ", err)
			}
			writer = bufio.NewWriter(outfile)
			m += 1
		}
		line := scanner.Text() + "\n"
		writer.WriteString(line)
		i += len(line)
	}
	writer.Flush()
	outfile.Close()
}

func DoMap(JobNumber int, fileName string,
	nreduce int, Map func(string) *list.List) {
	// Your code here
}

func DoReduce(job int, fileName string, nmap int,
	Reduce func(string, *list.List) string) {
	// Your code here
}

// Merge the results of the reduce jobs using a k-way merge over sorted reduce outputs.
// You do not need to modify this function, as the code has already been provided
func (mr *MapReduce) Merge() {
	DPrintf("Merge phase")

	runs := make([][]KeyValue, 0, mr.nReduce)
	for i := 0; i < mr.nReduce; i++ {
		p := MergeName(mr.file, i)
		DPrintf("Merge: read %s\n", p)
		runs = append(runs, readFilePairs(p))
	}

	// Track current index in each run.
	idx := make([]int, len(runs))

	out, err := os.Create(mr.outputFile)
	if err != nil {
		log.Fatal("Merge: ", err)
	}
	defer out.Close()

	w := bufio.NewWriter(out)
	defer w.Flush()

	// K-way merge: repeatedly emit the smallest current key across runs.
	for {
		minRun := -1
		var minKey string

		for i := 0; i < len(runs); i++ {
			if idx[i] >= len(runs[i]) {
				continue
			}
			k := runs[i][idx[i]].Key
			if minRun == -1 || k < minKey {
				minRun = i
				minKey = k
			}
		}

		if minRun == -1 {
			break // all runs exhausted
		}

		kv := runs[minRun][idx[minRun]]
		if _, err := w.WriteString(fmt.Sprintf("%s: %s\n", kv.Key, kv.Value)); err != nil {
			log.Fatal("Merge: ", err)
		}
		idx[minRun]++
	}
}

func (mr *MapReduce) CleanupFiles() {
	matches, err := filepath.Glob("mrtmp.*.tmp")
	if err != nil {
		log.Fatal("CleanupFiles: ", err)
	}

	for _, path := range matches {
		removeFileIfExists(path)
	}
	// Keep final output file if debug is on
	if Debug == 0 && mr.outputFile != "" {
		removeFileIfExists(mr.outputFile)
	}
}

func (mr *MapReduce) CleanupRegistration() {
	args := &ShutdownArgs{}
	var reply ShutdownReply
	ok := call(mr.MasterAddress, "MapReduce.Shutdown", args, &reply)
	if !ok {
		fmt.Printf("Cleanup: RPC %s error\n", mr.MasterAddress)
	}
	DPrintf("CleanupRegistration: done\n")
}

// Run jobs in parallel, assuming a shared file system
func (mr *MapReduce) Run() {
	DPrintf("Run mapreduce job %s %s\n", mr.MasterAddress, mr.file)

	mr.Split(mr.file)
	mr.stats = mr.RunMaster()
	mr.Merge()
	mr.CleanupRegistration()

	DPrintf("%s: MapReduce done\n", mr.MasterAddress)

	mr.DoneChannel <- true
}
