package main

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const expectedKJV12SHA256 = "94687d5e7b2bf7e6899e9521abd705ac02e044f2e8c6d7aa744a58fc6694df9b"

func readVerifiedKJV12Content() string {
	b, err := os.ReadFile("kjv12.txt")
	if err != nil {
		panic(fmt.Sprintf("failed to read kjv12.txt: %v", err))
	}
	h := sha256.Sum256(b)
	got := hex.EncodeToString(h[:])
	if got != expectedKJV12SHA256 {
		panic(fmt.Sprintf("unexpected kjv12.txt hash: got %s, want %s", got, expectedKJV12SHA256))
	}
	return string(b)
}

func listToKeyValues(l *list.List) []KeyValue {
	out := make([]KeyValue, 0, l.Len())
	for e := l.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(KeyValue))
	}
	return out
}

func keyValueMultiset(pairs []KeyValue) map[KeyValue]int {
	out := make(map[KeyValue]int)
	for _, kv := range pairs {
		out[kv]++
	}
	return out
}

func parseCountLines(lines []string) map[string]int {
	out := make(map[string]int)
	for _, line := range lines {
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			panic(fmt.Sprintf("invalid count line format: %q", line))
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			panic(fmt.Sprintf("invalid count line value: %q (%v)", line, err))
		}
		out[parts[0]] = n
	}
	return out
}

func topNCountLines(counts map[string]int, n int) []string {
	type kv struct {
		key   string
		count int
	}
	items := make([]kv, 0, len(counts))
	for k, c := range counts {
		items = append(items, kv{key: k, count: c})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].count != items[j].count {
			return items[i].count < items[j].count
		}
		return items[i].key < items[j].key
	})

	if n > len(items) {
		n = len(items)
	}
	items = items[len(items)-n:]

	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, fmt.Sprintf("%s: %d", it.key, it.count))
	}
	return out
}

func randomWord(rng *rand.Rand, minLen int, maxLen int) string {
	n := minLen + rng.Intn(maxLen-minLen+1)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + rng.Intn(26))
	}
	return string(b)
}

func randomInputText(rng *rand.Rand, words int) (string, []string) {
	separators := []string{" ", "  ", "\n", "\t", ",", ".", "!", "?", ";", ":", "-"}
	var sb strings.Builder
	var wordList []string
	for i := 0; i < words; i++ {
		w := randomWord(rng, 1, 8)
		wordList = append(wordList, w)
		sb.WriteString(w)
		if i != words-1 {
			sb.WriteString(separators[rng.Intn(len(separators))])
		}
	}
	return sb.String(), wordList
}

func TestPartI_MapSmallInput(t *testing.T) {
	got := listToKeyValues(Map("Hello, world! Hello"))
	want := []KeyValue{
		{Key: "Hello", Value: "1"},
		{Key: "world", Value: "1"},
		{Key: "Hello", Value: "1"},
	}

	if !reflect.DeepEqual(keyValueMultiset(got), keyValueMultiset(want)) {
		t.Fatalf("Map() mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestPartI_ReduceSmallInput(t *testing.T) {
	values := list.New()
	values.PushBack("1")
	values.PushBack("1")
	values.PushBack("1")

	got := Reduce("apple", values)
	want := "apple: 3"

	if got != want {
		t.Fatalf("Reduce() mismatch: want %q, got %q", want, got)
	}
}

func TestPartI_DoMapReduceSmallInput(t *testing.T) {
	got := DoMapReduce("cat dog cat")
	sort.Strings(got)

	want := []string{"cat: 2", "dog: 1"}
	sort.Strings(want)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DoMapReduce() mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestPartI_DoMapReduceKJV12Input(t *testing.T) {
	content := readVerifiedKJV12Content()
	wantTop := []string{
		"unto: 8940",
		"he: 9666",
		"shall: 9760",
		"in: 12334",
		"that: 12577",
		"And: 12846",
		"to: 13384",
		"of: 34434",
		"and: 38850",
		"the: 62075",
	}

	got := DoMapReduce(content)
	if len(got) == 0 {
		t.Fatalf("DoMapReduce() produced no output for kjv12.txt")
	}

	gotCounts := parseCountLines(got)
	gotTop := topNCountLines(gotCounts, len(wantTop))

	if !reflect.DeepEqual(gotTop, wantTop) {
		t.Fatalf("top KJV counts mismatch\nwant: %#v\ngot:  %#v", wantTop, gotTop)
	}
}

func TestPartI_MapRandomInput(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	input, wantWords := randomInputText(rng, 800000)

	got := listToKeyValues(Map(input))
	if len(got) != len(wantWords) {
		t.Fatalf("Map() random count mismatch: want %d, got %d", len(wantWords), len(got))
	}

	wantCounts := make(map[string]int)
	for _, w := range wantWords {
		wantCounts[w]++
	}
	gotCounts := make(map[string]int)

	for _, kv := range got {
		if kv.Value != "1" {
			t.Fatalf("Map() random mismatch: expected value %q, got %q for key %q", "1", kv.Value, kv.Key)
		}
		gotCounts[kv.Key]++
	}

	if !reflect.DeepEqual(gotCounts, wantCounts) {
		t.Fatalf("Map() random aggregate mismatch")
	}
}

func TestPartI_ReduceRandomInput(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	key := randomWord(rng, 3, 10)
	count := 1000 + rng.Intn(300000)

	values := list.New()
	for i := 0; i < count; i++ {
		values.PushBack("1")
	}

	got := Reduce(key, values)
	want := fmt.Sprintf("%s: %d", key, count)
	if got != want {
		t.Fatalf("Reduce() random mismatch: want %q, got %q", want, got)
	}
}

func TestPartI_DoMapReduceRandomInput(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	input, words := randomInputText(rng, 120000)

	wantCounts := make(map[string]int)
	for _, w := range words {
		wantCounts[w]++
	}

	got := DoMapReduce(input)
	gotCounts := parseCountLines(got)

	if !reflect.DeepEqual(gotCounts, wantCounts) {
		var mismatches []string
		for k, wc := range wantCounts {
			if gc, ok := gotCounts[k]; !ok {
				mismatches = append(mismatches, fmt.Sprintf("missing key %q (want count %d)", k, wc))
			} else if gc != wc {
				mismatches = append(mismatches, fmt.Sprintf("key %q: want %d, got %d", k, wc, gc))
			}
			if len(mismatches) >= 5 {
				break
			}
		}
		for k, gc := range gotCounts {
			if _, ok := wantCounts[k]; !ok {
				mismatches = append(mismatches, fmt.Sprintf("extra key %q (got count %d)", k, gc))
			}
			if len(mismatches) >= 5 {
				break
			}
		}
		t.Fatalf("DoMapReduce() random aggregate mismatch (first %d issues):\n%s",
			len(mismatches), strings.Join(mismatches, "\n"))
	}
}

func TestPartI_DoMapReduceSingleWord(t *testing.T) {
	got := DoMapReduce("hello")
	want := []string{"hello: 1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DoMapReduce() mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestPartI_DoMapReduceCaseSensitive(t *testing.T) {
	got := DoMapReduce("Hello hello HELLO")
	sort.Strings(got)
	want := []string{"HELLO: 1", "Hello: 1", "hello: 1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DoMapReduce should be case-sensitive\nwant: %v\ngot:  %v", want, got)
	}
}

func TestPartI_MapWhitespaceOnly(t *testing.T) {
	got := listToKeyValues(Map("   \t\n\n  "))
	if len(got) != 0 {
		t.Fatalf("Map() with only whitespace should return empty, got %d items", len(got))
	}
}

func TestPartI_ReduceSingleValue(t *testing.T) {
	values := list.New()
	values.PushBack("1")
	got := Reduce("word", values)
	want := "word: 1"
	if got != want {
		t.Fatalf("Reduce() mismatch: want %q, got %q", want, got)
	}
}

func TestPartI_MapNoLetters(t *testing.T) {
	got := listToKeyValues(Map("123 456 !@# $%^"))
	if len(got) != 0 {
		t.Fatalf("Map() with no letters should return empty, got %d items", len(got))
	}
}
