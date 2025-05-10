package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

func getMetadata() (int, int) {
	args := MetadataArgs{}
	reply := MetadataReply{}
	ok := call("Coordinator.Metadata", args, &reply)
	if !ok {
		log.Fatal("get metadata failed")
	}
	return reply.NMap, reply.NReduce
}

// Request for a task from coordinator
func getTask() *Task {
	args := TaskRequestArgs{}
	reply := TaskRequestReply{}
	ok := call("Coordinator.Assign", args, &reply)
	if !ok {
		log.Fatal("request task failed")
	}
	return reply.Task
}

func updateTask(task *Task) {
	args := TaskUpdateArgs{task}
	reply := TaskUpdateReply{}
	ok := call("Coordinator.Update", args, &reply)
	if !ok {
		log.Fatal("update task failed")
	}
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	nMap, nReduce := getMetadata()

	for {
		task := getTask()
		if task == nil {
			time.Sleep(100 * time.Millisecond)
		} else if task.Type == MapTaskType {
			// perform Map operation on input
			file, err := os.Open(task.Filename)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			file.Close()
			kva := mapf(task.Filename, string(content))

			// persist intermediate results in files
			encoders := make(map[int]*json.Encoder)
			for r := range nReduce {
				filename := fmt.Sprintf("mr-tmp/mr-%v-%v", task.ID, r)
				f, err := os.OpenFile(filename, os.O_RDWR, 0644)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				defer f.Close()
				encoders[r] = json.NewEncoder(f)
			}
			for _, kv := range kva {
				r := ihash(kv.Key) % nReduce
				err := encoders[r].Encode(&kv)
				if err != nil {
					log.Fatalf("writing to file failed: %v", err)
				}
			}

			task.Status = CompletedStatus
			updateTask(task)
		} else { // task.Type == ReduceTaskType
			kva := []KeyValue{}
			for m := range nMap {
				filename := fmt.Sprintf("mr-tmp/mr-%v-%v", m, task.ID)
				f, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				defer f.Close()
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%v", task.ID)
			ofile, _ := os.Create(oname)
			defer ofile.Close()

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}

			task.Status = CompletedStatus
			updateTask(task)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
