package mr

import (
	"time"
)

type TaskType string
type TaskStatus string
type AssignPhase string

const (
	//Define task type
	Map    TaskType = "MapTask"
	Reduce TaskType = "ReduceTask"

	//Define task status
	Ready    TaskStatus = "Ready"
	Running  TaskStatus = "Running"
	Finished TaskStatus = "Finished"

	//Define AssignPhase
	MapPhase    AssignPhase = "MapPhase"
	ReducePhase AssignPhase = "ReducePhase"

	//Define tmp files and final files directories
	TmpMapFilePath      = "/tmp/tmp_map_file/"
	TmpReduceFilePath   = "/tmp/tmp_reduce_file/"
	FinalMapFilePath    = "/tmp/final_map_file/"
	FinalReduceFilePath = "/tmp/final_reduce_file/"

	//Define task expired time
	TaskExpiredTime = 10
)

type Task struct {
	TaskType       TaskType //Map or Reduce
	MapWorkerId    int      //If worker in map phase, given by master
	ReduceWorkerId int      //If worker in reduce phase, given by master
	InputFile      string   //Input file for map task
	BeginTime      time.Time
	TaskStatus     TaskStatus
}

// Worker Request master for task
type TaskArgs struct {
	WorkerId int
}

// Master Response to worker
type TaskReply struct {
	Task       *Task
	ReducerNum int //Number of reducers
	AllDone    bool
}

//Mapper reports to master that map task should be done
type MapTaskDoneArgs struct {
	MapWorkerId int
	Files	   []string //Intermediate files for different reducer
}

//Master Response to mapper
type MapTaskDoneReply struct {
	Err error // nil if the task done is confirmed by master
}

//Reducer reports to master that reduce task should be done
type ReduceTaskDoneArgs struct {
	ReduceWorkerId int
	File string //Intermediate files for different reducer
}

type ReduceTaskDoneReply struct {
	Err error // nil if the task done is confirmed by master
}


// key-val pair for intermediate
type KeyValue struct {
	Key   string
	Value string
}

// intermediate
type ByKey []KeyValue

// For sort the intermediate
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
