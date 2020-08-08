package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

//file status
const (
	UnAllocated = iota
	Allocated
	Finished
)

var mapTask chan string
var reduceTask chan int

// Master...
type Master struct {
	BeginningFile       []string
	MapTaskNum          int
	BeginningFileStatus map[string]int
	MapFinished         bool

	NReduce         int
	InterFile       [][]string
	InterFileStatus map[int]int
	ReduceFinished  bool

	RWLock *sync.RWMutex
}

//RPC handler...
func (m *Master) HandleCallForTask(args *MyArgs, reply *MyReply) error {
	if args.MessageType == MsgForStartTask {
		reply.TaskType = ""
		select {
		case fileName := <-mapTask:
			println("Machine Name:", args.MachineName, " ==> "+"Start doing Map Task at file ------> FileName:|", fileName, "|")
			reply.Filename = fileName
			reply.MapNumAllocated = m.MapTaskNum
			reply.TaskType = "map"
			reply.NReduce = m.NReduce
			m.RWLock.Lock()
			m.BeginningFileStatus[fileName] = Allocated
			m.MapTaskNum++
			m.RWLock.Unlock()
			go m.checkCrash("map", fileName)
			return nil
		case number := <-reduceTask:
			println("Machine Name:", args.MachineName, " ==> "+"Start doing Reduce Task at file --------> FileName:| mr-*-", strconv.Itoa(number), "|")
			reply.ReduceFileList = m.InterFile[number]
			reply.TaskType = "reduce"
			reply.NReduce = m.NReduce
			reply.ReduceNumAllocated = number
			m.RWLock.Lock()
			m.InterFileStatus[number] = Allocated
			m.RWLock.Unlock()
			go m.checkCrash("reduce", strconv.Itoa(number))
			return nil
		}
	} else if args.MessageType == MsgForFinishMap {
		println("Machine Name:", args.MachineName, " ==> "+"Finish doing Map Task at file -------> FileName:|", args.FileName, "|")
		m.RWLock.Lock()
		m.BeginningFileStatus[args.FileName] = Finished
		m.RWLock.Unlock()
	} else if args.MessageType == MsgForFinishReduce {
		println("Machine Name:", args.MachineName, " ==> "+"Finish doing Reduce Task at file -----> FileName:| mr-*-", args.FileName, "|")
		println("Machine Name:", args.MachineName, " ==> "+"Create Final out file ----------------> FileName:| mr-out-", args.FileName, "|")
		m.RWLock.Lock()
		index, _ := strconv.Atoi(args.FileName)
		m.InterFileStatus[index] = Finished
		m.RWLock.Unlock()
	}
	return nil
}

//fualt tolerant
func (m *Master) checkCrash(taskType, file string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if taskType == "map" {
				m.RWLock.Lock()
				m.BeginningFileStatus[file] = UnAllocated
				m.RWLock.Unlock()
				mapTask <- file
			} else if taskType == "reduce" {
				m.RWLock.Lock()
				index, _ := strconv.Atoi(file)
				m.InterFileStatus[index] = UnAllocated
				m.RWLock.Unlock()
				reduceTask <- index
			}
			return
		default:
			if taskType == "map" {
				m.RWLock.RLock()
				if m.BeginningFileStatus[file] == Finished {
					m.RWLock.RUnlock()
					return
				}
				m.RWLock.RUnlock()
			} else if taskType == "reduce" {
				m.RWLock.RLock()
				index, _ := strconv.Atoi(file)
				if m.InterFileStatus[index] == Finished {
					m.RWLock.RUnlock()
					return
				}
				m.RWLock.RUnlock()
			}
		}
	}
}

func (m *Master) checkMapStatus() bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _, v := range m.BeginningFileStatus {
		if v == UnAllocated || v == Allocated {
			return false
		}
	}
	return true
}

func (m *Master) checkReduceStatus() bool {
	m.RWLock.RLock()
	defer m.RWLock.RUnlock()
	for _, v := range m.InterFileStatus {
		if v == UnAllocated || v == Allocated {
			return false
		}
	}
	return true
}

func (m *Master) HandleCallForInterFile(args *MyIntermediateFile, reply *MyReply) error {
	m.RWLock.Lock()
	println("Machine Name:", args.MachineName, " ==> "+"Create Intermediate file ------> FileName:|", args.FileName, "|")
	m.InterFile[args.Index] = append(m.InterFile[args.Index], args.FileName)
	m.RWLock.Unlock()
	reply.TaskType = "done"
	return nil
}

//a thread to generate Task
func (m *Master) generateTask() {
	for k, v := range m.BeginningFileStatus {
		if v == UnAllocated {
			mapTask <- k
		}
	}
	ok := false
	for !ok {
		ok = m.checkMapStatus()
	}
	m.MapFinished = true
	println("\n<-------All Map Task Finish------->\n")

	for k, v := range m.InterFileStatus {
		if v == UnAllocated {
			reduceTask <- k
		}
	}
	ok2 := false
	for !ok2 {
		ok2 = m.checkReduceStatus()
	}
	m.ReduceFinished = true
	println("\n<-------All Reduce Task Finish------->\n")

}

//listen to work
func (m *Master) server() {
	mapTask = make(chan string, 5)
	reduceTask = make(chan int, 5)
	rpc.Register(m)
	rpc.HandleHTTP()
	go m.generateTask()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.

func (m *Master) Done() bool {
	ret := false
	ret = m.ReduceFinished
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.BeginningFile = files
	m.BeginningFileStatus = make(map[string]int)
	m.MapFinished = false
	m.MapTaskNum = 0
	m.NReduce = nReduce
	m.InterFile = make([][]string, nReduce)
	m.InterFileStatus = make(map[int]int)
	m.ReduceFinished = false
	m.RWLock = new(sync.RWMutex)

	for _, fileName := range m.BeginningFile {
		m.BeginningFileStatus[fileName] = UnAllocated
	}
	for i := 0; i < nReduce; i++ {
		m.InterFileStatus[i] = UnAllocated
	}
	m.server()
	return &m
}
