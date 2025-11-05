package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	nextFile   *int
	inputFiles []string
	lock       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RequestTask(args interface{}, reply *Split) error {

	m.lock.Lock()
	reply.filename = m.inputFiles[*m.nextFile]
	reply.taskNum = *m.nextFile

	*m.nextFile++

	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	// l, e := net.Listen("unix", "mr-socket")
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
