package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType string
type TaskStatus string
type AssignPhase string

const (
	//Define task type
	Map TaskType = "MapTask"
	Reduce TaskType = "ReduceTask"

	//Define task status
	Ready TaskStatus = "Ready"
	Running TaskStatus = "Running"
	Finished TaskSatus= "Finished"

	//Define AssignPhase
	MapPhase AssignPhase = "MapPhase"
	ReducePhase AssignPhase = "ReducePhase"

	//Define tmp files and final files directories
	TmpMapFilePath = "/tmp/tmp_map_file/"
	TmpReduceFilePath = "/tmp/tmp_reduce_file/"
	FinalMapFilePath = "/tmp/final_map_file/"
	FinalReduceFilePath = "/tmp/final_reduce_file/"

	//Define task expired time
	TaskExpiredTime = 10 	
)


type Task struct{
	TaskType 		TaskType 	//Map or Reduce
	MapWorkerId 	int  		//If worker in map phase, given by master
	ReduceWorkerId 	int 		//If worker in reduce phase, given by master
	InputFile 		string 		//Single file in map. File pattern in reduce
	BeginTime 		time.Time	
	TaskStatus 		TaskStatus	//Ready, Running, Finished. For worker, it should always be running
}

type TaskArgs struct{
	WorkerId 	int
}
type TaskReply struct{
	Task 		Task		
	ReduceNum 	int		//the number of reducer, so the mapper can seperate intermediate for different reducer
	AllDone 	bool	//Check if all task done. If all done, worker should exit, otherwise loop request master for task
}

type MapTaskDoneArgs struct{
	MapWorkerId int
	Files 		[]string //Intermediate files for different reducer
}

type MapTaskDoneReply struct{
	Err error //nil if the task done is confirmed by master
}

//Reducer reports to master that reduce task should be done
type ReduceTaskDoneArgs struct{
	ReduceWorkerId 	int
	file 			string
}

type ReduceTaskDoneReply struct{
	Err error //Master reply for reducer's reduce task done request
}


//Key-Val pair for intermediate
type KeyValue struct {
	Key   string
	Value string
}

//Intermediate
type ByKey []KeyValue


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}


func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Bykey) Less(i, j int) bool { return a[i].Key < a[j].Key }
