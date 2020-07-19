package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TimeoutDuration = time.Duration(10 * time.Second)

type TaskState string

const(
	Idle TaskState = "Idle"
	InProgress TaskState = "InProgress"
	Done TaskState = "Done"
)

type MapTask struct {
	num int
	filename string
	state TaskState
	timeout time.Time
}

type ReduceTask struct {
	num int
	state TaskState
	timeout time.Time
}

type Master struct {
	lock sync.Mutex
	nReduce int
	mapDone bool
	reduceDone bool
	mapTasks []MapTask
	reduceTasks []ReduceTask
}


func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	fmt.Printf("Received request to get task.\n")
	m.lock.Lock()
	defer m.lock.Unlock()

	// Are there any timed-out tasks we can reassign?
	m.clearTimeouts()

	if !m.mapDone {
		taskNum := m.getNextMapTask()
		if taskNum != -1 {
			task := m.mapTasks[taskNum]
			fmt.Printf("Assigning map task %d (%v) to worker\n", task.num, task.filename)

			reply.TaskType = MapTaskType
			reply.Filename = task.filename
			reply.TaskNum = task.num
			reply.NumReduceTasks = m.nReduce

			task.state = InProgress
			task.timeout = time.Now().Add(TimeoutDuration)
			m.mapTasks[taskNum] = task

			return nil
		}

		reply.TaskType = WaitTaskType
		return nil
	} else if !m.reduceDone {
		taskNum := m.getNextReduceTask()
		if taskNum != -1 {
			task := m.reduceTasks[taskNum]
			fmt.Printf("Assigning reduce task %d to worker\n", task.num)
			reply.TaskType = ReduceTaskType
			reply.TaskNum = taskNum

			task.state = InProgress
			task.timeout = time.Now().Add(TimeoutDuration)
			m.reduceTasks[taskNum] = task

			return nil
		}

		reply.TaskType = WaitTaskType
		return nil
	}

	// Done
	reply.TaskType = DieTaskType
	return nil
}

func (m *Master) clearTimeouts() {
	if !m.mapDone {
		for i, task := range m.mapTasks {
			if task.state == InProgress && time.Now().After(task.timeout)  {
				fmt.Printf("Map task %d timed out\n", task.num)
				task.state = Idle
				m.mapTasks[i] = task
			}
		}
	}

	if !m.reduceDone {
		for i, task := range m.reduceTasks {
			if task.state == InProgress && time.Now().After(task.timeout)  {
				fmt.Printf("Reduce task %d timed out\n", task.num)
				task.state = Idle
				m.reduceTasks[i] = task
			}
		}
	}
}

func (m *Master) getNextMapTask() int {
	for _, task := range m.mapTasks {
		if task.state == Idle {
			return task.num
		}
	}
	return -1
}

func (m *Master) getNextReduceTask() int {
	for _, task := range m.reduceTasks {
		if task.state == Idle {
			return task.num
		}
	}
	return -1
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	// TODO: only allow reporting from the assigned worker

	m.lock.Lock()
	fmt.Printf("Received a task report: %v task %d is %v\n", args.TaskType, args.TaskNum, args.TaskState)
	defer m.lock.Unlock()

	taskType := args.TaskType
	taskNum := args.TaskNum
	taskState := args.TaskState

	if taskType == MapTaskType {
		task := m.mapTasks[taskNum]
		task.state = taskState
		m.mapTasks[taskNum] = task
		m.mapDone = m.isMapDone()
	} else if taskType == ReduceTaskType {
		task := m.reduceTasks[taskNum]
		task.state = taskState
		m.reduceTasks[taskNum] = task
		m.reduceDone = m.isReduceDone()
	}

	return nil
}

func (m *Master) isMapDone() bool {
	for _, task := range m.mapTasks {
		if task.state != Done {
			return false
		}
	}
	return true
}

func (m *Master) isReduceDone() bool {
	for _, task := range m.reduceTasks {
		if task.state != Done {
			return false
		}
	}
	return true
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
	fmt.Printf("Master is listening on %v...", sockname)
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
	fmt.Printf("Making Master. nReduce = %d\n", nReduce)

	m := Master{}
	m.nReduce = nReduce
	m.mapDone = false
	m.reduceDone = false

	// Create map tasks from files
	for i, filename := range files {
		fmt.Printf("Creating task for file %v\n", filename)
		task := MapTask{}
		task.num = i
		task.filename = filename
		task.state = Idle
		m.mapTasks = append(m.mapTasks, task)
	}

	// Create reduce tasks from param
	i := 0
	for i < nReduce {
		task := ReduceTask{}
		task.num = i
		task.state = Idle
		m.reduceTasks = append(m.reduceTasks, task)
		i += 1
	}

	m.server()
	return &m
}
