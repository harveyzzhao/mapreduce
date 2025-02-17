package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MapTask struct {
	ID       int
	Status   string // "notstarted" | "inprogress" | "completed"
	Filename string
}

type ReduceTask struct {
	ID     int
	Status string // "notstarted" | "inprogress" | "completed"
	Key    string
}

type Coordinator struct {
	// Your definitions here.
	Mu          sync.Mutex
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask
	NReduce     int
	State       string // "map | reduce | completed"
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTaskHandler(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.State == "map" {
		task := c.getNotStartedMapTask()
		if task != nil {
            task.Status = "inprogress"
            
			reply.TaskType = "map"
			reply.TaskID = task.ID
			reply.Filename = task.Filename
			reply.NReduce = c.NReduce
			return nil
		} else if !c.allMapTasksCompleted() {
			reply.TaskType = "wait"
			// TaskID, Filename and NReduce fields unused
			return nil
		} else {
			c.State = "reduce"
		}
	}

	if c.State == "reduce" {
		task := c.getNotStartedReduceTask()
		if task != nil {
            task.Status = "inprogress"

			reply.TaskType = "reduce"
			reply.TaskID = task.ID
			// reduce tasks do not use Filename and NReduce fields
            return nil
		} else if !c.allReduceTasksCompleted() {
			reply.TaskType = "wait"
			// TaskID, Filename and NReduce fields unused
			return nil
		} else {
			c.State = "completed"
		}
	}

	// if the program is here, it means
	// we have processed all map and reduce tasks.
	// tell worker to shutdown
	if c.State == "completed" {
		reply.TaskType = "shutdown"
		return nil
	}

	return fmt.Errorf("unexpected coordinator state: %s", c.State)
}

func (c *Coordinator) getNotStartedMapTask() *MapTask {
	for _, task := range(c.MapTasks) {
        if task.Status == "notstarted" {
            return task
        }
    }
    return nil
}

func (c *Coordinator) allMapTasksCompleted() bool {
    for _, task := range(c.MapTasks) {
        if task.Status != "completed" {
            return false
        }
    }
    return true
}

func (c *Coordinator) getNotStartedReduceTask() *ReduceTask {
    for _, task := range(c.ReduceTasks) {
        if task.Status == "notstarted" {
            return task
        }
    }
    return nil
}

func (c *Coordinator) allReduceTasksCompleted() bool {
    for _, task := range(c.ReduceTasks) {
        if task.Status != "completed" {
            return false
        }
    }
    return true
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
        MapTasks: CreateMapTasks(files),
        ReduceTasks: []*ReduceTask{},
        NReduce: nReduce,
        State: "map",
    }

	c.server()
	return &c
}

func CreateMapTasks(files []string) []*MapTask {
    tasks := []*MapTask{}
    for i, file := range(files) {
        tasks = append(tasks, &MapTask{
            ID: i,
            Status: "notstarted",
            Filename: file,
        })
    }
    return tasks
}