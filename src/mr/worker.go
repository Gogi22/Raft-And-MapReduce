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
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the master.
	// CallExample()
	fmt.Println("starting up Worker")

	task := GetTask()

	// fmt.Println("got Result", task)

	if task.Mapt != nil {
		fmt.Println("starting Map and NReduce is", task.Mapt.NReduce)
		files, err := RunMap(mapf, *task.Mapt)

		if err != nil {
			fmt.Println("error in worker", err)
			return
		}

		fmt.Println(files)

		args := MapDoneArgs{}
		args.Filename = task.Mapt.Filename
		args.IntermediateFiles = files
		reply := MapDoneReply{}

		call("Master.MapFinish", args, &reply)

		return
	} else if task.Reducet != nil {
		fmt.Println("starting Reduce")
		err := RunReduce(reducef, *task.Reducet)
		if err != nil {
			fmt.Println("Error in reduce is", err)
			return
		}
		args := ReduceDoneArgs{}
		args.ReduceId = task.Reducet.ReduceId
		call("Master.ReduceFinish", &args, &ReduceDoneReply{})
	} else {
		return
	}

}

func RunReduce(reducef func(string, []string) string, task ReduceTask) error {
	kva := []KeyValue{}
	for _, filename := range task.IntermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	// oname := "mr-out-0"
	oname := "temp-*"
	ofile, err := ioutil.TempFile("./", oname)

	if err != nil {
		return err
	}

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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()

	os.Rename(ofile.Name(), fmt.Sprintf("../main/mr-out-%d.txt", task.ReduceId))
	return nil
}

func RunMap(mapf func(string, string) []KeyValue, task MapTask) (map[int][]string, error) {
	file, err := os.Open("../main/" + task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))

	partition := make(map[int][]KeyValue)
	for _, kv := range kva {
		key := ihash(kv.Key) % task.NReduce
		partition[key] = append(partition[key], kv)
	}
	filenames := make(map[int][]string)
	for i, arr := range partition {
		oname := fmt.Sprintf("mr-%d-%d.json", task.TaskId, i)
		filenames[i] = append(filenames[i], oname)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range arr {
			if err := enc.Encode(&kv); err != nil {
				return map[int][]string{}, err
			}
		}
		ofile.Close()
	}

	return filenames, nil
}

//
// example function to show how to make an RPC call to the master.
//

func GetTask() TaskRequestReply {

	fmt.Println("calling master for Task")
	args := Args{}
	reply := TaskRequestReply{}

	call("Master.GetTask", &args, &reply)

	return reply
}

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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
