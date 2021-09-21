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
	Unassigned = 0
	InProgress = 1
	Finished   = 2
)

type Master struct {
	// Your definitions here.
	MapTaskId         int
	MapTasks          map[string]int
	IntermediateFiles map[int][]string
	ReduceTasks       map[int]int
	nReduce           int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *Args, reply *TaskRequestReply) error {

	mapTask := m.ChooceMapTask()

	if mapTask != nil {
		reply.Mapt = mapTask
		fmt.Println(reply.Mapt, mapTask)
		fmt.Println("in master", reply.Reducet)
		return nil
	}

	if !m.MapStageDone() {
		return nil
	}

	fmt.Println("maps are done")

	reduceTask := m.ChooseReduceTask()

	if reduceTask != nil {
		reply.Reducet = reduceTask
		fmt.Println(reply.Reducet, reduceTask)
		return nil
	}

	return nil
}

func (m *Master) MapFinish(args *MapDoneArgs, reply *MapDoneReply) error {
	m.MapTasks[args.Filename] = Finished

	for i := 0; i < m.nReduce; i++ {
		m.IntermediateFiles[i] = append(m.IntermediateFiles[i], args.IntermediateFiles[i]...)
	}
	fmt.Println(m.IntermediateFiles)
	return nil
}

func (m *Master) ReduceFinish(task *ReduceTask, reply *Reply) error {
	m.ReduceTasks[task.ReduceId] = Finished
	fmt.Println(task.ReduceId, "done")
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//code here -- RPC handlers for the worker to call.
func (m *Master) ChooceMapTask() *MapTask {
	var task *MapTask = nil

	for i, f := range m.MapTasks {
		if f == Unassigned {
			task = &MapTask{}
			task.Filename = i
			task.TaskId = m.MapTaskId
			task.NReduce = m.nReduce
			m.MapTasks[i] = InProgress
			m.MapTaskId++
			break
		}
	}
	return task
}

func (m *Master) ChooseReduceTask() *ReduceTask {
	var task *ReduceTask = nil
	fmt.Println("choosing reduce task", m.ReduceTasks)

	for i, j := range m.ReduceTasks {
		if j == Unassigned {
			fmt.Println("reduce id", j)
			task = &ReduceTask{}
			task.ReduceId = i
			task.IntermediateFiles = m.IntermediateFiles[i]
			m.ReduceTasks[i] = InProgress
			break
		}
	}

	return task
}

func (m *Master) MapStageDone() bool {
	for _, mp := range m.MapTasks {
		if mp != Finished {
			return false
		}
	}
	return true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.MapTaskId = 1
	m.MapTasks = make(map[string]int)
	m.ReduceTasks = make(map[int]int)
	m.IntermediateFiles = make(map[int][]string)
	// for _, s := range files {
	// 	m.MapTasks[s] = Unassigned
	// }
	m.MapTasks[files[0]] = Unassigned
	m.nReduce = nReduce
	for i := 0; i < m.nReduce; i++ {
		m.IntermediateFiles[i] = []string{}
		m.ReduceTasks[i] = 0
	}

	m.server()
	return &m
}
