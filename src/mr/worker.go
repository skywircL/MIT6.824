package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"net/rpc"
	"time"
	"io"
	"os"
)

// Map functions return a slice of KeyValue.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	// Your worker implementation here.
	for {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMap(mapf, task)
				DoneTask(task)
			}
		case CompleteTask:
			{
				//exit
				return 
			}
		case ReduceTask:
			{
				DoReduceTask(reducef,task)
				DoneTask(task)
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
}

func DoneTask(task Task) Task {
	fmt.Printf("testMap:%d %d %v\n",task.TaskNum,task.TaskType,time.Now())
	args := FinishArgs{
		Ts :task,
	}
	reply := Task{}
	ok := call("Coordinator.FinishedTask", &args, &reply)

	if ok {
		fmt.Println(time.Now(),reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}


func DoMap(mapf func(string, string) []KeyValue, raw Task) {//这里应该有并发问题,已经解决
	//ps： 因为都在GFS里，所以知道了filename就可以直接读(自认为)
	file, err := os.Open(raw.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", raw.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", raw.Filename)
	}
	file.Close()
	
	kva := mapf(raw.Filename, string(content))
	
	//intermediate = append(intermediate, kva...)
	//create intermediate file  what's the mean of k and y

	// use json
	//提前创建好文件
	dir, _ := os.Getwd()
	
	Mapfiles := []*os.File{}	
	for i:=0;i<raw.ReduceSum;i++{
		oname,_:=os.CreateTemp(dir,"")
		//newfile, _ := os.Create("mr-" + fmt.Sprint(raw.TaskNum)+ "-" + fmt.Sprint(i)) //use ihash to confirm reduceNum
		Mapfiles = append(Mapfiles,oname)
	}

	for _, v := range kva {  //如果文件存在会将所有文件内容清空，所以不能每次都用create创建一个
		//newfile, _ := os.Create("mr-" + fmt.Sprint(raw.TaskNum)+ "-" + fmt.Sprint(ihash(v.Key)%raw.ReduceSum)) //use ihash to confirm reduceNum
		f := Mapfiles[ihash(v.Key)%raw.ReduceSum]
		enc := json.NewEncoder(f)
		err := enc.Encode(&v)
		if err != nil {
			log.Println(err)
		}
	}
	for i,v:=range Mapfiles{
		err := os.Rename(v.Name(),fmt.Sprintf("mr-%d-%d", raw.TaskNum, i)) //use ihash to confirm reduceNum
		if err!= nil {
			log.Println(err)
		}
	}
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoReduceTask(reducef func(string, []string) string,raw Task){
	//coordinater告知reduceNum，直接从本地读取
	//一次将所有的file都读出来
	kva := []KeyValue{}
	for i:=0;i<raw.MapSum;i++ {
		fn :=  fmt.Sprintf("mr-%d-%d",i,raw.TaskNum)
		file, err := os.Open(fn)
		if err != nil {
			log.Fatalf("cannot open %v", fn)
		}
		dec := json.NewDecoder(file)
		for {
		  var kv KeyValue
		  if err := dec.Decode(&kv); err != nil {
			break
		  }
		  kva = append(kva, kv)
		}
		file.Close()	
	}

	//shuffle
	sort.Sort(ByKey(kva))
	dir, _ := os.Getwd()
	oname,_:=os.CreateTemp(dir,"")
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(oname, "%v %v\n", kva[i].Key, output)
		i = j
	}
	oname.Close()
	os.Rename(oname.Name(),fmt.Sprintf("mr-out-%d",raw.TaskNum))

}


func GetTask() Task {

	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.GetTask", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
