package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type Master struct {
	// Your definitions here.
	nextFile   int
	inputFiles []string
	lock       sync.Mutex
	tasks      map[string]string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RequestTask(args Empty, reply *Split) error {

	// m.lock.Lock()
	fileName := m.inputFiles[m.nextFile]
	f, err := filepath.Abs(fileName)

	if err != nil {
		panic(err)
	}

	reply.Filename = f
	reply.TaskNum = m.nextFile

	//TODO: will add state data "idle", "completed", "failed"
	m.tasks[strconv.Itoa(m.nextFile)] = f //register task
	m.nextFile++
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	// l, e := net.Listen("unix", "mr-socket")

	log.Printf("listening on: %v", ":1234")

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.inputFiles = files
	// Your code here.
	m.server()

	return &m
}
