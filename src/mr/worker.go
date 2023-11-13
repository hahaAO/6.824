package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var myClientId ClientId

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
	// 将时间戳和进程ID转换myClientId
	myClientId = strconv.FormatInt(time.Now().UnixNano(), 10) + strconv.Itoa(os.Getpid())
	// fmt.Printf("myClientId %s in \n", myClientId)

	for {
		taskType := CallAskTask()
		switch taskType {
		case TypeMap:
			reply := CallAskMap()
			if len(reply.InputFilename) == 0 {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			var intermediateFiles []*os.File
			var intermediateFileNames []string
			for i := 0; i < reply.ReduceNum; i++ {
				intermediateFileName := fmt.Sprintf("mr-%s-%d", myClientId, i)
				intermediateFileNames = append(intermediateFileNames, intermediateFileName)
				file, err := os.OpenFile(intermediateFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				intermediateFiles = append(intermediateFiles, file)
				if err != nil {
					// 打开文件失败，输出错误信息
					fmt.Println("Error opening file1:", err)
					return
				}
			}
			defer func() {
				for _, file := range intermediateFiles {
					file.Close()
				}
			}()
			for _, inputFilename := range reply.InputFilename {
				file, err := os.Open(inputFilename)
				if err != nil {
					log.Fatalf("cannot open %v", inputFilename)
				}
				defer file.Close()
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", inputFilename)
				}
				kva := mapf(inputFilename, string(content))
				writeContent := make(map[int]string, 10)
				for _, kv := range kva {
					writeContent[ihash(kv.Key)%reply.ReduceNum] += (kv.Key + " " + kv.Value + "\n")
				}
				for k, content := range writeContent {
					if _, err := intermediateFiles[k].WriteString(content); err != nil {
						// 写入文件失败，输出错误信息
						fmt.Println("Error writing to file:", err)
						os.Exit(1)
					}
				}
			}
			CallCompleteTask(TypeMap, reply.InputFilename, intermediateFileNames, nil)
		case TypeReduce:
			reply := CallAskReduce()
			if len(reply.IntermediateFiles) == 0 {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			// read intermediateFile
			intermediateFileMap := make(map[string][]string)
			for _, intermediateFile := range reply.IntermediateFiles {
				file, err := os.Open(intermediateFile)
				if err != nil {
					fmt.Println("Error opening file2:", intermediateFile, " error: ", err)
					return
				}
				defer file.Close()
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					parts := strings.Split(line, " ")
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])
					intermediateFileMap[key] = append(intermediateFileMap[key], value)
				}
				// 检查是否有错误发生
				if err := scanner.Err(); err != nil {
					fmt.Println("scan Error!!!!  file: ", intermediateFile, " error: ", err)
					os.Exit(1)
				}

			}
			// open mr-out File
			outFilename := "mr-out-" + strconv.Itoa(reply.CompleteReduceTaskId)
			outFile, err := os.OpenFile(outFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Println("Error opening file:", err)
				os.Exit(1)
			}
			defer outFile.Close()

			// write mr-out File
			writeContent := ""
			for k, v := range intermediateFileMap {
				reducefRes := reducef(k, v)
				writeContent += fmt.Sprintf("%s %s\n", k, reducefRes)
				CallAskTask() // just ping
			}
			// 所有计算成功才能写而不是每次计算都写
			outFile.WriteString(writeContent)

			// fmt.Println("CallCompleteTask(TypeReduce, 1): ClientId ", myClientId)
			CallCompleteTask(TypeReduce, nil, nil, &reply.CompleteReduceTaskId)
		case TypeWait:
			time.Sleep(20 * time.Millisecond)
		case TypeDown:
			return

		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallAskTask() TaskType {
	args := AskTaskArgs{Id: myClientId}
	reply := AskTaskReply{}
	call("Coordinator.AskTask", &args, &reply)
	return reply.TType
}

func CallAskMap() AskMapReply {
	args := AskMapArgs{Id: myClientId}
	reply := AskMapReply{}
	call("Coordinator.AskMap", &args, &reply)
	return reply
}

func CallAskReduce() AskReduceReply {
	args := AskReduceArgs{Id: myClientId}
	reply := AskReduceReply{}
	call("Coordinator.AskReduce", &args, &reply)
	return reply
}

func CallCompleteTask(tType TaskType, inputFileNames []string, completeMapFileNames []string,
	completeReduce *int) CompleteTaskReply {
	args := CompleteTaskArgs{
		Id:                   myClientId,
		TType:                tType,
		InputFileNames:       inputFileNames,
		CompleteMapFileNames: completeMapFileNames,
		CompleteReduceTaskId: completeReduce,
	}
	reply := CompleteTaskReply{}
	call("Coordinator.CompleteTask", &args, &reply)
	return reply
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
