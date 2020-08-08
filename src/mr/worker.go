package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := CallForTask(MsgForStartTask, "")
		if reply.TaskType == "" {
			break
		}
		switch reply.TaskType {
		case "map":
			processeMap(&reply, mapf)
			CallForTask(MsgForFinishMap, reply.Filename)
		case "reduce":
			processeReduce(&reply, reducef)
			CallForTask(MsgForFinishReduce, strconv.Itoa(reply.ReduceNumAllocated))
		}
	}
}

func processeMap(reply *MyReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	kva := mapf(reply.Filename, string(content))
	kvaNew := partition(kva, reply)
	for i, kvSli := range kvaNew {
		newInterFile := createJosn(kvSli, reply.MapNumAllocated, i)
		err = updataInterFileSystem(newInterFile, i)
		if err != nil {
			log.Fatalf("Error of sending interfile")
		}
	}
}

func processeReduce(reply *MyReply, reducef func(string, []string) string) {
	outFileContent := []KeyValue{}
	for _, fileName := range reply.ReduceFileList {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			outFileContent = append(outFileContent, kv)
		}
	}
	sort.Sort(ByKey(outFileContent))
	oname := "mr-out-" + strconv.Itoa(reply.ReduceNumAllocated)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(outFileContent) {
		j := i + 1
		for j < len(outFileContent) && outFileContent[j].Key == outFileContent[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, outFileContent[k].Value)
		}
		output := reducef(outFileContent[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", outFileContent[i].Key, output)

		i = j
	}
}

func updataInterFileSystem(newInterFile string, i int) error {
	args := MyIntermediateFile{}
	args.MachineName = strconv.Itoa(os.Getpid())
	args.FileName = newInterFile
	args.Index = i
	reply := MyReply{}
	call("Master.HandleCallForInterFile", &args, &reply)
	if reply.TaskType == "done" {
		return nil
	}
	return fmt.Errorf("Error of sending interfile")
}

func partition(kva []KeyValue, reply *MyReply) [][]KeyValue {
	kvaNew := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.NReduce
		kvaNew[index] = append(kvaNew[index], kv)
	}
	return kvaNew
}

func createJosn(kvSli []KeyValue, mapNum int, reduceNum int) string {
	fileName := "mr-" + strconv.Itoa(mapNum) + "-" + strconv.Itoa(reduceNum)
	ofile, _ := os.Create(fileName)
	enc := json.NewEncoder(ofile)
	for _, kv := range kvSli {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode:")
		}
	}
	ofile.Close()
	return fileName
}

// example function to show how to make an RPC call to the master.
// the RPC argument and reply types are defined in rpc.go.
func CallForTask(MessageType int, fileName string) MyReply {
	// declare an argument structure.
	args := MyArgs{}
	args.MachineName = strconv.Itoa(os.Getpid())
	args.MessageType = MessageType
	args.FileName = fileName
	// declare a reply structure.
	reply := MyReply{}

	// send the RPC request, wait for the reply.
	call("Master.HandleCallForTask", &args, &reply)
	return reply
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	if err.Error() != "unexpected EOF" {
		fmt.Println("error: ", err)
	}
	return false
}
