package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// const perMapTaskLineNum int = 100

var inputFilenames []string

type TaskState int

const (
	waiting TaskState = iota
	doing
	complete
	crash
)

var reduceNum int
var mapTaskState map[string]TaskState    // key is filename
var reduceTaskState map[int]TaskState    // key is suffix of intermediate files ,aka reduce task id
var intermediateFileMap map[int][]string // key if reduce task id,value is intermediateFiles

var mapTaskWaitingNum int
var mapTaskDoingNum int = 0

var mapTaskCompleteNum int = 0
var mapTaskNum_M sync.Mutex

var reduceWaitingTaskNum int
var reduceTaskDoingNum int = 0

var reduceTaskCompletedNum int = 0
var reduceTaskNum_M sync.Mutex

const clientCrashTime = 15 * time.Second

var clientLastPingTime map[string]time.Time
var clientLastPingTime_M sync.Mutex

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	{
		clientLastPingTime_M.Lock()
		defer clientLastPingTime_M.Unlock()
		clientLastPingTime[args.Id] = time.Now()
	}

	mapTaskNum_M.Lock()
	defer mapTaskNum_M.Unlock()
	if mapTaskWaitingNum > 0 {
		reply.TType = TypeMap
		return nil
	}

	reduceTaskNum_M.Lock()
	defer reduceTaskNum_M.Unlock()

	if reduceWaitingTaskNum > 0 && mapTaskDoingNum <= 0 {
		reply.TType = TypeReduce
		return nil
	}
	if mapTaskDoingNum > 0 || reduceTaskDoingNum > 0 {
		reply.TType = TypeWait
		return nil
	}
	reply.TType = TypeDown
	return nil
}

func (c *Coordinator) AskMap(args *AskMapArgs, reply *AskMapReply) error {
	clientLastPingTime_M.Lock()
	defer clientLastPingTime_M.Unlock()
	clientLastPingTime[args.Id] = time.Now()

	mapTaskNum_M.Lock()
	defer mapTaskNum_M.Unlock()

	reply.ReduceNum = reduceNum

	if mapTaskWaitingNum == 0 {
		return nil
	} else {
		for filename, state := range mapTaskState {
			if state == waiting {
				reply.InputFilename = append(reply.InputFilename, filename)
				mapTaskState[filename] = doing
				go func(doingFilename string, clientId string) {
					for {
						time.Sleep(clientCrashTime)
						shouldBreak := func() bool {
							mapTaskNum_M.Lock()
							defer mapTaskNum_M.Unlock()
							if mapTaskState[doingFilename] == doing {
								clientLastPingTime_M.Lock()
								defer clientLastPingTime_M.Unlock()
								if time.Since(clientLastPingTime[clientId]) > clientCrashTime {
									mapTaskState[doingFilename] = waiting
									mapTaskWaitingNum++
									mapTaskDoingNum--
									return true
								}
								return false
							} else { //  this Task is Complete
								return true
							}
						}()
						if shouldBreak {
							return
						}
					}
				}(filename, args.Id)
				mapTaskWaitingNum--
				mapTaskDoingNum++
				return nil
			}
		}
	}
	// else client doing
	return nil
}

func (c *Coordinator) AskReduce(args *AskReduceArgs, reply *AskReduceReply) error {
	clientLastPingTime_M.Lock()
	clientLastPingTime[args.Id] = time.Now()
	clientLastPingTime_M.Unlock()

	reduceTaskNum_M.Lock()
	defer reduceTaskNum_M.Unlock()

	fmt.Printf("AskReduce: in\n")
	if reduceWaitingTaskNum == 0 {
		fmt.Printf("reduceWaitingTaskNum == 0")
		return nil
	}
	fmt.Printf("AskReduce for start\n")

	for reduceTaskId, state := range reduceTaskState {
		if state == waiting {
			reduceTaskState[reduceTaskId] = doing
			reply.IntermediateFiles = intermediateFileMap[reduceTaskId]
			fmt.Printf("reply.IntermediateFiles: %v\n", reply.IntermediateFiles)
			reduceWaitingTaskNum--
			reduceTaskDoingNum++
			go func(doingReduceTaskId int, clientId string) {
				for {
					time.Sleep(clientCrashTime)
					shouldBreak := func() bool {
						reduceTaskNum_M.Lock()
						defer reduceTaskNum_M.Unlock()
						if reduceTaskState[doingReduceTaskId] == doing {
							clientLastPingTime_M.Lock()
							defer clientLastPingTime_M.Unlock()
							if time.Since(clientLastPingTime[clientId]) > clientCrashTime {
								reduceTaskState[doingReduceTaskId] = waiting
								reduceWaitingTaskNum++
								reduceTaskDoingNum--
								return true
							}
							reduceTaskState[doingReduceTaskId] = crash
							return false
						} else { //  this Task is Complete or crash
							return true
						}

					}()
					if shouldBreak {
						return
					}
				}
			}(reduceTaskId, args.Id)
			return nil
		}
	}
	// else client doing
	fmt.Printf("AskReduce else client doing\n")
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	clientLastPingTime_M.Lock()
	clientLastPingTime[args.Id] = time.Now()
	clientLastPingTime_M.Unlock()

	mapTaskNum_M.Lock()
	defer mapTaskNum_M.Unlock()
	reduceTaskNum_M.Lock()
	defer reduceTaskNum_M.Unlock()

	switch args.TType {
	case TypeMap:
		mapTaskDoingNum--
		mapTaskCompleteNum++
		// fmt.Printf("args.InputFileNames: %v\n", args.InputFileNames)
		// fmt.Printf("args.CompleteMapFileNames: %v\n", args.CompleteMapFileNames)

		for _, fileName := range args.InputFileNames {
			mapTaskState[fileName] = complete
		}
		for _, fileName := range args.CompleteMapFileNames {
			if reduceTaskId, err := strconv.Atoi(fileName[len(fileName)-1:]); err != nil {
				panic(fmt.Sprintf("fileName name %s error reduceTaskId: %d \n", fileName, reduceTaskId))
			} else {
				intermediateFileMap[reduceTaskId] = append(intermediateFileMap[reduceTaskId], fileName)
			}
		}
	case TypeReduce:
		reduceTaskDoingNum--
		reduceTaskCompletedNum++
		if completeReduceTaskId := args.CompleteReduceTaskId; completeReduceTaskId != nil {
			reduceTaskState[*completeReduceTaskId] = complete
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	time.Sleep(2 * time.Second)

	// Your code here.
	mapTaskNum_M.Lock()
	defer mapTaskNum_M.Unlock()
	reduceTaskNum_M.Lock()
	defer reduceTaskNum_M.Unlock()

	fmt.Println("mapTaskWaitingNum: ", mapTaskWaitingNum)
	fmt.Println("mapTaskDoingNum: ", mapTaskDoingNum)
	fmt.Println("mapTaskCompleteNum: ", mapTaskCompleteNum)

	fmt.Println("reduceWaitingTaskNum: ", reduceWaitingTaskNum)
	fmt.Println("reduceTaskDoingNum: ", reduceTaskDoingNum)
	fmt.Println("reduceTaskCompletedNum: ", reduceTaskCompletedNum)

	if mapTaskWaitingNum == 0 && mapTaskDoingNum == 0 && reduceWaitingTaskNum == 0 && reduceTaskDoingNum == 0 {
		return true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	clientLastPingTime = make(map[string]time.Time)

	inputFilenames = os.Args[1:]
	mapTaskWaitingNum = len(inputFilenames)
	mapTaskState = make(map[string]TaskState)
	reduceTaskState = make(map[int]TaskState)
	intermediateFileMap = make(map[int][]string, 0)

	i := 0
	for _, filename := range inputFilenames {
		// fmt.Println("filename ", filename)
		// fmt.Println("i ", i)
		mapTaskState[filename] = waiting
		reduceTaskState[i] = waiting
		i++
	}
	reduceNum = nReduce
	reduceWaitingTaskNum = nReduce

	// for _, filename := range os.Args[1:] {
	// 	file, err := os.Open(filename)
	// 	if err != nil {
	// 		log.Fatalf("cannot open %v", filename)
	// 	} else {
	// 		log.Printf("open %v\n", filename)
	// 	}
	// 	defer file.Close()

	// 	content, err := ioutil.ReadAll(file)
	// 	if err != nil {
	// 		log.Fatalf("cannot read %v", filename)
	// 	}
	// 	file.Close()

	// 	// lines = append(lines, content)

	// 	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	// 	for scanner.Scan() {
	// 		// WARNING Bytes will make mistake! why?
	// 		line := scanner.Text()
	// 		lines = append(lines, line)
	// 	}
	// 	if err := scanner.Err(); err != nil {
	// 		fmt.Println("Error reading input:", err)
	// 	}
	// }

	// mapTaskWaitingNum = len(lines)

	c.server()
	return &c
}
