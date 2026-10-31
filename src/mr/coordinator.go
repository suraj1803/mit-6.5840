package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)


type Task struct {
	TaskId int
	TaskType string
	TaskState string
	InputFile string
}

type Coordinator struct {
	Tasks []Task
	Files []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//


func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}



func (c *Coordinator) TestHandler(args *TestArgs, reply *TestArgs) error {
	for idx, task := range c.Tasks {
		if task.TaskState == "idle" {

			c.Tasks[idx].TaskState = "active"

			reply.FileName = task.InputFile;
			return nil
		}
	}

	return errors.New("All tasks are ongoing")
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	for _, file := range files {
		c.Files = append(c.Files, file)
	}

	for i, file  := range c.Files {
		c.Tasks = append(c.Tasks, Task {
			TaskId: i,
			TaskType: "map",
			TaskState: "idle",
			InputFile: file,
		})

		// /* DEBUG */
		// task := c.Tasks[i]
		// fmt.Printf("TaskId: %v\nTaskType: %v\nTaskState: %v\nInputFile: %v\n\n", task.TaskId, task.TaskType, task.TaskState, task.InputFile)
	}

	c.server()
	return &c
}
