package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	IdleStatus      TaskStatus = "idle"
	PendingStatus   TaskStatus = "pending"
	CompletedStatus TaskStatus = "completed"

	MapTaskType    TaskType = "map"
	ReduceTaskType TaskType = "reduce"
)

type TaskStatus string
type TaskType string

type Coordinator struct {
	Completed   int
	Files       []string
	MapTasks    []Task
	ReduceTasks []Task
}

type Task struct {
	Filename string
	ID       int
	Status   TaskStatus
	Type     TaskType
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Metadata(args *MetadataArgs, reply *MetadataReply) error {
	reply.NMap = len(c.MapTasks)
	reply.NReduce = len(c.ReduceTasks)
	return nil
}

// Assigns available tasks to workers upon request. If there's any idle Map
// tasks, assign the next one; When all Map tasks are completed, assign
// Reduce tasks.
func (c *Coordinator) Assign(args *TaskRequestArgs, reply *TaskRequestReply) error {
	for i, task := range c.MapTasks {
		if task.Status == IdleStatus {
			reply.Task = &task
			c.MapTasks[i].Status = PendingStatus
			return nil
		}
	}

	if c.Completed >= len(c.MapTasks) {
		for i, task := range c.ReduceTasks {
			if task.Status == IdleStatus {
				reply.Task = &task
				c.ReduceTasks[i].Status = PendingStatus
				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) CompleteTask(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	task := args.Task
	if task.Type == MapTaskType {
		c.MapTasks[task.ID].Status = CompletedStatus
	} else if task.Type == ReduceTaskType {
		c.ReduceTasks[task.ID].Status = CompletedStatus
	}
	c.Completed++
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
	return c.Completed == len(c.MapTasks)+len(c.ReduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files: files,
	}

	// create Map tasks
	for idx, file := range files {
		task := Task{
			Filename: file,
			ID:       idx,
			Status:   IdleStatus,
			Type:     MapTaskType,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	// create Reduce tasks
	for idx := range nReduce {
		task := Task{
			ID:     idx,
			Status: IdleStatus,
			Type:   ReduceTaskType,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	// create intermediate files
	for m := range len(files) {
		for r := range nReduce {
			fname := fmt.Sprintf("mr-%v-%v", m, r)
			os.Create(fname)
		}
	}

	c.server()
	return &c
}
