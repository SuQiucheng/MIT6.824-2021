package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
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

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type TaskType int

const(
	NoTask TaskType = iota
	MapTask
	ReduceTask
)
type Task struct {
	TaskType TaskType
	TaskId int
	NreduceNum int
	NmapNum int
	InputFile string
	WorkerId int
	DDL time.Time
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//RPC的请求参数
	var LastTaskType TaskType = NoTask
	var LastTaskId int = 0
	for {
		args := RequestArgs{
			WorkerId:     os.Getpid(),
			LastTaskType: LastTaskType,
			LastTaskId:   LastTaskId,
		}
		reply := AskWork(&args)
		//log.Printf("reply:%+v\n",reply)
		if reply.TaskType==MapTask {
			//log.Printf("Map任务开始:任务Id-%d，工作进程Id-%d...\n",reply.TaskId,os.Getegid())
			CallMapTask(reply,mapf)
		}else if reply.TaskType==ReduceTask{
			//log.Printf("Reduce任务开始:任务Id-%d，工作进程Id-%d...\n",reply.TaskId,os.Getegid())
			CallReduceTask(reply,reducef)
		}else if reply.TaskType==NoTask {
			//log.Printf("工作进程%d结束...",os.Getpid())
			break
		}
		LastTaskType = reply.TaskType
		LastTaskId = reply.TaskId
	}
	//log.Printf("Worker %s exit\n", os.Getpid())
}
func AskWork(args *RequestArgs) *WorkerReply{
	//log.Println("AskWork()...")
	//log.Printf("请求参数为%+v\n",args)
	reply := WorkerReply{}
	call("Coordinator.WorkHandle",&args,&reply)
	//log.Println("AskWork()完成...")
	return &reply

}
func AskFile(reply *WorkerReply) (fileNames []string) {
	for i:=0;i<reply.NmapNum;i++{
		file := fmt.Sprintf("mr-%d-%d",i,reply.TaskId)
		fileNames = append(fileNames,file)
	}
	return fileNames
}
func CallMapTask(reply *WorkerReply,mapf func(string, string) []KeyValue){
	//log.Println("MapTask is running...")
	filename := reply.FileName
	file, err := os.Open(filename)
	//log.Println(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	intermediate := make([][]KeyValue,reply.NreduceNum)

	for i:=0;i<len(kva);i++{
		index :=ihash(kva[i].Key)%reply.NreduceNum
		intermediate[index] = append(intermediate[index],kva[i])
	}
	for i:=0;i<reply.NreduceNum;i++{
		ifilename := fmt.Sprintf("mr-%d-%d-temp",reply.TaskId,i)
		ifile,err := os.Create(ifilename)
		if err != nil {
			log.Fatalf("cannot open %v", ifilename)
		}
		for _,kv:= range intermediate[i]{
			fmt.Fprintf(ifile, "%v\t%v\n", kv.Key, kv.Value)
		}
		ifile.Close()
	}
	//log.Printf("PID=%d线程的第%d个任务已经完成...\n", os.Getpid(), reply.TaskId)



}
func CallReduceTask(reply *WorkerReply,reducef func(string, []string) string){
	fileNames := AskFile(reply)
	var intermediate []KeyValue
	var lines []string
	for i:=0;i<len(fileNames);i++{
		file, err := os.Open(fileNames[i])
		if err!=nil {
			log.Fatalf("打开文件%s错误",fileNames[i])
		}


		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Failed to read map output file %s: %e", file, err)
		}
		lines = append(lines, strings.Split(string(content), "\n")...)

		file.Close()

	}

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		intermediate = append(intermediate, KeyValue{
			Key: parts[0],
			Value: parts[1],
		})
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d-temp",reply.TaskId)
	ofile,_ := os.Create(oname)
	i:= 0
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
