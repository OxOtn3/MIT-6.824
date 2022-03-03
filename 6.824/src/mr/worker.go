package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("[worker]: 实例化worker")
	// Your worker implementation here.
	// 轮询任务
	for {
		log.Println("[worker]: 发起rpc调用GetTask方法")
		task, ok := CallForTask()
		if !ok {
			// rpc调用异常
			log.Fatal("[worker]: GetTask的rpc调用错误,worker结束")
		}
		if task.TaskType == "" {
			// 没有任务
			// sleep1秒后再尝试获取
			log.Println("[worker]: 当前没有任务")
			time.Sleep(1 * time.Second)
			continue
		}
		// 获取到task
		log.Println("[worker]: 获取到", task.TaskType, "类型任务:", task.TaskId)
		if task.TaskType == "map" {
			// map任务
			intermediateFile := MapTaskFunc(task, mapf)
			log.Println("[worker]:", task.TaskType, "类型任务:", task.TaskId, "已完成")
			// 任务完成，通知coordinator
			args := Args{Task: *task, IntermediateFile: intermediateFile}
			ok := CallForFinish(&args)
			if !ok {
				log.Fatal("fail to CallForFinish")
			}
		} else {
			// reduce任务
			finalFile := ReduceTaskFunc(task, reducef)
			log.Println("[worker]:", task.TaskType, "类型任务:", task.TaskId, "已完成")
			// 通知coordinator
			args := &Args{Task: *task, FinalFile: finalFile}
			ok := CallForFinish(args)
			if !ok {
				log.Fatalf("fail to CallForFinish")
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func MapTaskFunc(task *Task, mapf func(string, string) []KeyValue) (intermediateFile []string) {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("open file %s failed", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("read file %s failed", filename)
	}
	file.Close()
	// 调用mapf处理得到kv对
	kvs := mapf(filename, string(content))
	// 每个中间文件对应的encoder的map
	encoderMap := make(map[int]*json.Encoder)
	for i := 0; i < task.NReduce; i++ {
		// A reasonable naming convention for intermediate files is `mr-X-Y`,
		// where X is the Map task number, and Y is the reduce task number
		name := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		file, err := os.Create(name)
		if err != nil {
			log.Fatalf("create file %s failed", file.Name())
		}
		intermediateFile = append(intermediateFile, name)
		// 为每个文件分配一个encoder
		encoderMap[i] = json.NewEncoder(file)
	}
	for _, kv := range kvs {
		// 获取映射后的分区Id
		partitionId := ihash(kv.Key) % task.NReduce
		encoder := encoderMap[partitionId]
		// 通过encoder写入文件
		err = encoder.Encode(kv)
		if err != nil {
			log.Fatalf("encode failed, error: %s", err.Error())
		}
	}
	return intermediateFile
}

func ReduceTaskFunc(task *Task, reducef func(string, []string) string) (finalFile string) {
	intermediateFile := task.IntermediateFile
	// kv对集合
	var kvs []KeyValue
	for _, filename := range intermediateFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("open file %s failed", filename)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			// 一直decode，直到读到EOF返回err
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	// 临时文件
	tmpFile, err := os.CreateTemp("", "mr-tmp*")
	if err != nil {
		log.Fatalf("fail to create tmpFile, err: %s", err)
	}
	// When a reduce worker has read all intermediate data,
	// it sorts it by the intermediate keys so that all occurrences of the same key are grouped together.
	// The sorting is needed because typically many different keys map to the same reduce task.
	sort.Sort(ByKey(kvs))
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
	finalFile = "mr-out-" + strconv.Itoa(task.TaskId)
	// To ensure that nobody observes partially written files in the presence of crashes,
	// the MapReduce paper mentions the trick of
	// using a temporary file and atomically renaming it once it is completely written.
	// You can use `ioutil.TempFile` to create a temporary file and `os.Rename` to atomically rename it.
	err = os.Rename(tmpFile.Name(), finalFile)
	if err != nil {
		log.Fatalf("fail to rename temp file, error: %s", err.Error())
	}
	tmpFile.Close()
	return finalFile
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallForTask() (*Task, bool) {
	args := &Args{}
	reply := &Reply{}
	ok := call("Coordinator.GetTask", args, reply)
	return &reply.Task, ok
}

func CallForFinish(args *Args) bool {
	reply := &Reply{}
	ok := call("Coordinator.FinishTask", args, reply)
	return ok
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
