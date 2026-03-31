package main

import (
	"container/list"
	"cs134-assignment1/mapreduce"
	"fmt"
	"os"
)

// Use this file to supply your own Map and Reduce functions for Part 2 if you would like to run Part 2 manually.
// You can also just use the provided test cases to test your code and ignore this file entirely.
// The Map and Reduce functions follow the same format as in the sequential version
// but can be called multiple times, and in any order, by the MapReduce framework.
// Also note that the distributed MapReduce system automatically outputs the result
// in key: value format, so your Reduce function should only output the value, not the key as well
// Otherwise the key will be duplicated in the final output.
func Map(value string) *list.List {
	return nil
}

func Reduce(key string, values *list.List) string {
	return ""
}

// Can be run in 2 ways:
// 1) Master (e.g., go run wc.go master x.txt localhost:7777 out.txt)
// 2) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778)
// Make sure you run the master before the worker
func main() {
	switch os.Args[1] {
		case "master":
			if len(os.Args) != 5 {
				fmt.Printf("%s: see usage comments in file\n", os.Args[0])
				return
			}
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[4], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		case "worker":
			if len(os.Args) != 4 {
				fmt.Printf("%s: see usage comments in file\n", os.Args[0])
				return
			}
			mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
		default:
			fmt.Printf("%s: see usage comments in file\n", os.Args[0])
			return
	}
}
