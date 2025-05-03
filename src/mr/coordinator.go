package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type TaskStatus string
type TaskType string

const (
	IdleStatus      TaskStatus = "idle"
	PendingStatus   TaskStatus = "pending"
	CompletedStatus TaskStatus = "completed"

	MapType    TaskType = "map"
	ReduceType TaskType = "reduce"
)

type Coordinator struct {
	Files []string
	// TODO: how do we know which worker is working on what task?
	Tasks []Task
}

type Task struct {
	File   string
	Status TaskStatus
	Type   TaskType
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Assign(args *TaskRequestArgs, reply *TaskRequestReply) error {
	for _, task := range c.Tasks {
		if task.Type == MapType && task.Status == IdleStatus {
			reply.File = task.File
			reply.Type = task.Type
			break
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files: files,
	}

	// create Map tasks
	for _, file := range files {
		mTask := Task{
			Status: IdleStatus,
			File:   file,
			Type:   MapType,
		}
		c.Tasks = append(c.Tasks, mTask)
	}

	c.server()
	return &c
}
