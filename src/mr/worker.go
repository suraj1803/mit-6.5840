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

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	CallTest(mapf, reducef)
}

func CallTest(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for true {
		args := TestArgs{}
		reply := TestArgs{}
		intermediate := []KeyValue{}
		ok := call("Coordinator.TestHandler", &args, &reply)
		fmt.Printf("reply: %+v\n", reply)


		if ok {
			fmt.Printf("RPC success: %v\n", ok)

			if (reply.TaskType == "map") {
				file, err := os.Open(reply.FileName)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", file)
				}
				file.Close()
				kva := mapf(file.Name(), string(content))
				intermediate = append(intermediate, kva...)

				// Divide intermediate keys into nReduce buckets
				buckets := make([][]KeyValue, reply.NReduce)
				for _, kv := range intermediate {
					y := ihash(kv.Key) % reply.NReduce
					buckets[y] = append(buckets[y], kv)
				}

				// Write each bucket to a temp file and atomically rename to mr-X-Y
				for y := 0; y < reply.NReduce; y++ {
					tempFile, err := os.CreateTemp(".", "mr-tmp-*")
					if err != nil {
						log.Fatalf("cannot create temp file: %v", err)
					}

					enc := json.NewEncoder(tempFile)
					for _, kv := range buckets[y] {
						if err := enc.Encode(&kv); err != nil {
							log.Fatalf("cannot encode kv: %v", err)
						}
					}
					tempFile.Close()

					oname := fmt.Sprintf("mr-%d-%d", reply.TaskId, y)
					if err := os.Rename(tempFile.Name(), oname); err != nil {
						log.Fatalf("cannot rename file to %v: %v", oname, err)
					}
				}

				doneArgs := TestArgs{TaskId: reply.TaskId, TaskType: "map"}
				doneReply := TestArgs{}
				call("Coordinator.TaskDone", &doneArgs, &doneReply)
			} else if reply.TaskType == "reduce" {
				// Read all mr-X-Y files for this reduce task (Y = reply.TaskId)
				for x := 0; x < reply.TotalTasks; x++ {
					iname := fmt.Sprintf("mr-%d-%d", x, reply.TaskId)
					file, err := os.Open(iname)
					if err != nil {
						log.Fatalf("cannot open %v: %v", iname, err)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					file.Close()
				}

				// Sort intermediate key-value pairs by key
				sort.Sort(ByKey(intermediate))

				// Create a temporary output file and write Reduce results
				tempFile, err := os.CreateTemp(".", "mr-out-tmp-*")
				if err != nil {
					log.Fatalf("cannot create temp file: %v", err)
				}

				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// Correct format for each line of Reduce output
					fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				tempFile.Close()

				// Atomically rename to mr-out-Y
				oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
				if err := os.Rename(tempFile.Name(), oname); err != nil {
					log.Fatalf("cannot rename file to %v: %v", oname, err)
				}

				doneArgs := TestArgs{TaskId: reply.TaskId, TaskType: "reduce"}
				doneReply := TestArgs{}
				call("Coordinator.TaskDone", &doneArgs, &doneReply)
			} else if reply.TaskType == "wait" {
				time.Sleep(time.Second)
			} else if reply.TaskType == "exit" {
				fmt.Printf("all tasks finished\n")
				break
			}

		} else {
			break
		}
	}
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
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
