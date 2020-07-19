package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"encoding/json"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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

func getReduceTaskNumber(key string, numTasks int) int {
	return ihash(key) % numTasks
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	fmt.Println("Worker starting.\n")
	time.Sleep(time.Second)

	for {
		task := GetTask()

		if task.TaskType == WaitTaskType {
			fmt.Printf("Master asked us to wait.\n")
		} else if task.TaskType == DieTaskType {
			fmt.Printf("Master asked us to die.\n")
			os.Exit(0)
		} else if task.TaskType == MapTaskType {
			fmt.Printf("Running mapf for task %d (%v)\n", task.TaskNum, task.Filename)
			result := mapf(task.Filename, readFile(task.Filename))
			fmt.Printf("Writing results for task %d (%v)\n", task.TaskNum, task.Filename)
			writeIntermediateFiles(result, task.TaskNum, task.NumReduceTasks)
			ReportTask(task.TaskType, task.TaskNum, Done)
		} else if task.TaskType == ReduceTaskType {
			// TODO
			fmt.Printf("Running reducef for task %d\n", task.TaskNum)
			os.Exit(1)
		}

		time.Sleep(time.Second)
	}
}

func GetTask() GetTaskReply {
	fmt.Printf("Asking for task...\n")
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	return reply
}

func ReportTask(taskType TaskType, taskNum int, taskState TaskState) ReportTaskReply {
	fmt.Printf("Reporting %v task %d as %v...\n", taskType, taskNum, taskState)
	args := ReportTaskArgs{
		TaskType: taskType,
		TaskNum: taskNum,
		TaskState: taskState,
	}
	reply := ReportTaskReply{}
	call("Master.ReportTask", &args, &reply)
	return reply
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func writeIntermediateFiles(pairs []KeyValue, taskNum int, numReduceTasks int) {
	// Create intermediate files
	filenames := make([]string, numReduceTasks)
	files := make([]*os.File, numReduceTasks)
	encoders := make([]*json.Encoder, numReduceTasks)
	reduceTask := 0
	for reduceTask < numReduceTasks {
		filename := getFilenameForTask(taskNum, reduceTask)
		ofile, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		filenames[reduceTask] = filename
		files[reduceTask] = ofile
		encoders[reduceTask] = json.NewEncoder(ofile)
		reduceTask += 1
	}

	// Go through KeyValue pairs and write to the appropriate file
	for _, kv := range pairs {
		reduceTaskNum := getReduceTaskNumber(kv.Key, numReduceTasks)
		encoder := encoders[reduceTaskNum]
		if err := encoder.Encode(kv); err != nil {
			log.Fatalf("cannot write JSON to file %v", filenames[reduceTask])
		}
	}

	// Close files
	for _, file := range files {
		if err := file.Close(); err != nil {
			log.Fatalf("cannot close file")
		}
	}
}

func getFilenameForTask(mapTask int, reduceTask int) string {
	return fmt.Sprintf("mr-%d-%d", mapTask, reduceTask)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

