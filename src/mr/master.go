package mr

import (
	// "fmt"

	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	UNASSIGNED = 0
	INPROGRESS = 1
	FINISHED   = 2
)

type Condition struct {
	startTime int64
	state     int
}

type Master struct {
	// Your definitions here.
	MapTaskId         int
	MapTasks          map[string]Condition
	IntermediateFiles map[int][]string
	ReduceTasks       map[int]Condition
	nReduce           int
	Finish            bool
	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *Args, reply *TaskRequestReply) error {
	m.Lock()
	defer m.Unlock()
	mapTask := m.ChooceMapTask()

	if mapTask != nil {
		reply.Mapt = mapTask
		// fmt.Println(reply.Mapt, mapTask)
		// fmt.Println("in master", reply.Reducet)
		return nil
	}

	if !m.MapStageDone() {
		return nil
	}

	// fmt.Println("maps are done")

	reduceTask := m.ChooseReduceTask()

	if reduceTask != nil {
		reply.Reducet = reduceTask
		// fmt.Println(reply.Reducet, reduceTask)
		return nil
	}

	if m.ReduceStageDone() {
		reply.Finished = true
		m.Finish = true
	}

	return nil
}

func (m *Master) MapFinish(args *MapDoneArgs, reply *MapDoneReply) error {
	m.Lock()
	defer m.Unlock()
	m.MapTasks[args.Filename] = Condition{0, FINISHED}

	for i := 0; i < m.nReduce; i++ {
		m.IntermediateFiles[i] = append(m.IntermediateFiles[i], args.IntermediateFiles[i]...)
	}
	// fmt.Println(m.IntermediateFiles)
	return nil
}

func (m *Master) ReduceFinish(task *ReduceTask, reply *Reply) error {
	m.Lock()
	defer m.Unlock()
	m.ReduceTasks[task.ReduceId] = Condition{0, FINISHED}
	// fmt.Println(task.ReduceId, "done")
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//code here -- RPC handlers for the worker to call.
func (m *Master) ChooceMapTask() *MapTask {
	var task *MapTask = nil

	for i, c := range m.MapTasks {
		if c.state == UNASSIGNED {
			task = &MapTask{}
			task.Filename = i
			task.TaskId = m.MapTaskId
			task.NReduce = m.nReduce
			m.MapTasks[i] = Condition{time.Now().Unix(), INPROGRESS}
			m.MapTaskId++
			break
		}
	}
	return task
}

func (m *Master) ChooseReduceTask() *ReduceTask {
	var task *ReduceTask = nil
	// fmt.Println("choosing reduce task", m.ReduceTasks)

	for i, c := range m.ReduceTasks {
		if c.state == UNASSIGNED {
			// fmt.Println("reduce id", j)
			task = &ReduceTask{}
			task.ReduceId = i
			task.IntermediateFiles = m.IntermediateFiles[i]
			m.ReduceTasks[i] = Condition{time.Now().Unix(), INPROGRESS}
			break
		}
	}

	return task
}

func (m *Master) MapStageDone() bool {
	for _, mp := range m.MapTasks {
		if mp.state != FINISHED {
			return false
		}
	}
	return true
}

func (m *Master) ReduceStageDone() bool {
	for _, redc := range m.ReduceTasks {
		if redc.state != FINISHED {
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

// func (m *Master) WorkersFinished() {
// 	m.Finish = true
// }

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has FINISHED.
//
func (m *Master) CheckIfCrashed() {
	for i, mp := range m.MapTasks {
		if mp.state == INPROGRESS && mp.startTime+10 < time.Now().Unix() {
			m.MapTasks[i] = Condition{0, UNASSIGNED}
		}
	}

	for i, redc := range m.ReduceTasks {
		if redc.state == INPROGRESS && redc.startTime+10 < time.Now().Unix() {
			m.ReduceTasks[i] = Condition{0, UNASSIGNED}
		}
	}
}

func (m *Master) Done() bool {
	// Your code here.
	m.Lock()
	defer m.Unlock()
	if m.Finish {
		return true
	}
	m.CheckIfCrashed()
	return false
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
	m.MapTasks = make(map[string]Condition)
	m.ReduceTasks = make(map[int]Condition)
	m.IntermediateFiles = make(map[int][]string)
	for _, s := range files {
		m.MapTasks[s] = Condition{0, UNASSIGNED}
	}
	// m.MapTasks[files[0]] = Unassigned
	m.nReduce = nReduce
	for i := 0; i < m.nReduce; i++ {
		m.IntermediateFiles[i] = []string{}
		m.ReduceTasks[i] = Condition{0, UNASSIGNED}
	}
	m.server()
	return &m
}
