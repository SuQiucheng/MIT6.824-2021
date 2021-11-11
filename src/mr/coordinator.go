package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatus int

const(
	MapStatus CoordinatorStatus = iota
	ReduceStatus
	FinishedStatus
)
type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex
	MapNum  int

	ReduceNum  int

	AvailableTask chan *Task
	//存储当前已经分配出去的任务，key为TaskId，value为任务
	TaskList map[int]*Task

	Status CoordinatorStatus
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

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	//log.Println("rpc.server()...")
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
	c.lock.Lock()
	if c.Status == FinishedStatus {
		return true
	}
	c.lock.Unlock()
	return ret
}
func (c *Coordinator) WorkHandle(args *RequestArgs, reply *WorkerReply) error {
	if args.LastTaskType != NoTask{
		c.lock.Lock()

		if t, ok := c.TaskList[args.LastTaskId];ok && args.WorkerId == t.WorkerId {
			//log.Printf("PID=%d线程的第%d个任务已经完成...\n", args.WorkerId, args.LastTaskId)
			if args.LastTaskType == MapTask {
				for i := 0; i < c.ReduceNum; i++ {
					iOldFilename := fmt.Sprintf("mr-%d-%d-temp", args.LastTaskId, i)
					iNewFilename := fmt.Sprintf("mr-%d-%d", args.LastTaskId, i)
					//log.Printf("文件%s改名为%s",iOldFilename,iNewFilename)
					os.Rename(iOldFilename, iNewFilename)
				}
			} else if args.LastTaskType == ReduceTask {
				oOldFileName := fmt.Sprintf("mr-out-%d-temp", args.LastTaskId)
				oNewFileName := fmt.Sprintf("mr-out-%d", args.LastTaskId)
				//log.Printf("文件%s改名为%s",oOldFileName,oNewFileName)
				os.Rename(oOldFileName, oNewFileName)
				//同时应该删除map文件
				//for i:=0;i<c.MapNum;i++{
				//	file := fmt.Sprintf("mr-%d-%d",i,args.LastTaskId)
				//	os.Remove(file)
				//}
			}
			//log.Printf("删除前TaskList：%+v\n",c.TaskList)
			delete(c.TaskList, args.LastTaskId)
			//log.Printf("删除后TaskList：%+v\n",c.TaskList)

		}
		//假如map任务都执行完成了，那就转为reduce状态，开始执行reduce任务
		if c.Status == MapStatus && len(c.TaskList) == 0 {
			for i := 0; i < c.ReduceNum; i++ {
				task :=  &Task{
					TaskType:   ReduceTask,
					TaskId:     i,
					NreduceNum: c.ReduceNum,
					NmapNum:    c.MapNum,
					WorkerId: -1,
				}
				c.TaskList[task.TaskId] = task
				c.AvailableTask <- task
			}
			//log.Printf("ReduceTask正在写入:%+v\n", c.AvailableTask)
			c.Status = ReduceStatus
		}else if c.Status==ReduceStatus && len(c.TaskList)==0{
			log.Println("MR任务完成了...")
			c.Status = FinishedStatus
			close(c.AvailableTask)
		}
		c.lock.Unlock()
	}
	//log.Println("正在分配任务...")
	taskTemp,ok := <- c.AvailableTask
	if !ok{
		reply.TaskType = NoTask
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	reply.TaskId = taskTemp.TaskId
	reply.TaskType = taskTemp.TaskType
	reply.FileName = taskTemp.InputFile
	reply.NreduceNum = taskTemp.NreduceNum
	reply.NmapNum = taskTemp.NmapNum


	taskTemp.WorkerId = args.WorkerId
	taskTemp.DDL = time.Now().Add(10*time.Second)
	c.TaskList[taskTemp.TaskId] = taskTemp
	//log.Printf("分配任务完成，任务为%+v,ok=%+v",taskTemp,ok)

	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//log.Println("makeCoordinator()...")
	c := Coordinator{}

	// Your code here.
	c.ReduceNum = nReduce
	c.MapNum = len(files)
	c.AvailableTask = make(chan *Task,int(math.Max(float64(len(files)),float64(nReduce))))
	c.TaskList = map[int]*Task{}
	c.Status = MapStatus
	for i := 0; i < len(files); i++ {

		task :=  &Task{
			TaskType:   MapTask,
			TaskId:     i,
			NreduceNum: nReduce,
			InputFile:  files[i],
			NmapNum:    len(files),
			WorkerId: -1,
		}
		c.TaskList[task.TaskId] = task
		c.AvailableTask <- task
	}
	//log.Printf("c:%+v\n", c)
	go func() {
		for{
			time.Sleep(500*time.Millisecond)
			c.lock.Lock()
			for _,v := range c.TaskList{
				//假如workedId不为-1，且
				if v.WorkerId != -1 && time.Now().After(v.DDL) {
					v.WorkerId = -1
					//超时之后，将它放回原队列任务中
					c.AvailableTask<-v
				}
			}
			c.lock.Unlock()
		}
	}()
	c.server()
	return &c
}
