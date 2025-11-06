package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	inputFile, err := getTask()

	if err != nil {
		panic(err)
	}

	file, err := os.Open(inputFile)

	if err != nil {
		log.Fatalf("cannot open %v", inputFile)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}
	kva := mapf(inputFile, string(content))
	intermediate = append(intermediate, kva...)

}

func getTask() (string, error) {
	args := Empty{}
	reply := Split{}
	call("Master.RequestTask", args, &reply)
	fmt.Printf("file obtained -> %s\nnum -> %d\n", reply.Filename, reply.TaskNum)

	if _, err := os.Stat(reply.Filename); os.IsNotExist(err) {
		return "", err
	}
	return reply.Filename, nil
}

// example function to show how to make an RPC call to the master.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
