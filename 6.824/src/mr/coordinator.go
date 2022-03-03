package mr

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapTask    map[int]*Task
	ReduceTask map[int]*Task
	// 互斥锁保证并发安全
	lock        sync.Mutex
	MapChan     chan *Task
	ReduceChan  chan *Task
	MapFinished bool
	// 中间文件集合 reduceId->IntermediateFile
	// format: mr-X-Y, X为map的id, Y为reduce的id
	IntermediateFile map[int][]string
}

// Your code here -- RPC handlers for the worker to call.
type Task struct {
	// 任务ID
	TaskId int
	// map任务还是reduce任务
	TaskType string
	// 任务状态: idle, inProgress, completed
	TaskStatus string
	// MapTask的文件名
	Filename string
	// MapTask的中间文件个数
	NReduce int
	// ReduceTask的中间文件名
	IntermediateFile []string
}

func (s *Task) Completed() bool {
	return s.TaskStatus == "completed"
}

func (s *Task) InProgress() bool {
	return s.TaskStatus == "inProgress"
}

func (s *Task) Idle() bool {
	return s.TaskStatus == "idle"
}

func (s *Task) MakeCompleted() {
	s.TaskStatus = "completed"
}

func (s *Task) MakeInProgress() {
	s.TaskStatus = "inProgress"
}

func (s *Task) MakeIdle() {
	s.TaskStatus = "idle"
}

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

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Println("[coordinator]: 接受任务请求")
	if !c.MapFinished {
		// 防止没有map任务无法退出导致锁无法释放，先判断
		if len(c.MapChan) != 0 {
			task := <-c.MapChan
			task.MakeInProgress()
			reply.Task = *task
			log.Println("[coordinator]: 分配map任务:", task.TaskId)
			// 协程10秒后检查任务是否完成
			go c.CheckFinish(task.TaskId, task.TaskType)
		}
		return nil
	}
	// map任务已经全部完成
	//log.Println("[coordinator]: map任务已经全部完成")
	// 防止没有reduce任务无法退出导致锁无法释放，先判断
	if len(c.ReduceChan) != 0 {
		task := <-c.ReduceChan
		task.MakeInProgress()
		task.IntermediateFile = c.IntermediateFile[task.TaskId]
		reply.Task = *task
		log.Println("[coordinator]: 分配reduce任务:", task.TaskId)
		go c.CheckFinish(task.TaskId, task.TaskType)
	}
	return nil
}

func (c *Coordinator) FinishTask(args *Args, reply *Reply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	task := args.Task
	var t *Task
	if task.TaskType == "map" {
		t = c.MapTask[task.TaskId]
	} else {
		t = c.ReduceTask[task.TaskId]
	}
	if t.InProgress() {
		t.MakeCompleted()
		// 如果是map任务被完成,则需要创建一个reduce任务,则需要取出传来的中间文件名
		if t.TaskType == "map" {
			files := t.IntermediateFile
			for _, name := range files {
				split := strings.Split(name, "-")
				reduceId, _ := strconv.Atoi(split[2])
				c.IntermediateFile[reduceId] = append(c.IntermediateFile[reduceId], name)
			}
			return nil
		}
	}
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	mapDone := true
	reduceDone := true
	// 先判断map任务
	for _, task := range c.MapTask {
		if !task.Completed() {
			mapDone = false
			break
		}
	}
	c.MapFinished = mapDone
	if !mapDone {
		return false
	}
	// 再判断reduce任务
	for _, task := range c.ReduceTask {
		if !task.Completed() {
			reduceDone = false
			break
		}
	}
	return reduceDone
	// Your code here.

}

func (c *Coordinator) CheckFinish(taskId int, taskType string) {
	log.Println("[coordinator]: 协程检查10s后", taskType, "任务:", taskId, "是否完成")
	time.Sleep(10 * time.Second)
	c.lock.Lock()
	defer func() {
		c.lock.Unlock()
		log.Println("[coordinator]: 已释放lock互斥锁")
	}()
	if taskType == "map" {
		task := c.MapTask[taskId]
		if task.Completed() {
			log.Println("[coordinator]: 10s后", taskType, "任务:", taskId, "已完成")
			return
		} else {
			log.Println("[coordinator]: 10s后", taskType, "任务:", taskId, "未完成,重新加入处理该任务")
			// 未完成，释放任务
			task.MakeIdle()
			// 重新放到chan里
			c.MapChan <- task
		}
	} else {
		task := c.ReduceTask[taskId]
		if task.Completed() {
			log.Println("[coordinator]: 10s后", taskType, "任务:", taskId, "已完成")
			return
		} else {
			log.Println("[coordinator]: 10s后", taskType, "任务:", taskId, "未完成,重新加入处理该任务")
			task.MakeIdle()
			c.ReduceChan <- task
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTask:          make(map[int]*Task),
		ReduceTask:       make(map[int]*Task),
		MapChan:          make(chan *Task, 10000),
		ReduceChan:       make(chan *Task, nReduce),
		MapFinished:      false,
		IntermediateFile: make(map[int][]string),
	}

	// Your code here.
	// 初始化map任务
	for i, file := range files {
		// 实例化task
		task := &Task{
			TaskId:     i,
			TaskType:   "map",
			TaskStatus: "idle",
			Filename:   file,
			NReduce:    nReduce,
		}
		c.MapTask[i] = task
		c.MapChan <- task
	}
	log.Println("[coordinator]: 完成map任务初始化")

	// 初始化reduce任务
	for i := 0; i < nReduce; i++ {
		// 实例化task
		task := &Task{
			TaskId:           i,
			TaskType:         "reduce",
			TaskStatus:       "idle",
			IntermediateFile: make([]string, 0),
			NReduce:          nReduce,
		}
		c.ReduceTask[i] = task
		c.ReduceChan <- task
	}
	log.Println("[coordinator]: 完成reduce任务初始化")

	c.server()
	return &c
}
