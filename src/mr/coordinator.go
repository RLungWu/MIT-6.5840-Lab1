package mr

import(
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"time"
)

// Your code here -- RPC handlers for the worker to call.
type Coordinator struct {
	MapChannel 		chan *Task	//The channel with buffer to maintain map tasks
	ReduceChannel 	chan *Task	//The channel with buffer to maintain reduce tasks
	Files 			[]string	//The input files list, each file corresponds to a map worker
	MapperNum 		int			//The number of map workers, equal to the number of input files
	ReducerNum 		int			//The number of reduce workers, equal by the main function
	AssignPhase 	AssignPhase	//The current working phase: map or reduce
	MapWorkerId 	int			//The counter for map task, starting from 0
	ReduceWorkerId 	int			//The counter for reduce task, starting from 0
	AllMapDone 		bool 		//The flag to indicate all map tasks are done
	AllReduceDone 	bool 		//The flag to indicate all reduce tasks are done
	Lock 			sync.Mutex	//The lock to protect coordinatos' member variable(MapChannel, ReduceChannel, Files, MapperNum, ReducerNum, AssignPhase, MapWorkerId, ReduceWorkerId, AllMapDone, AllReduceDone)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

//Initialize the coordinater
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Pringln("MakeCoordinator")


	c := Coordinator{}
	c.MapChannel = make(chan *Task, len(files))
	c.ReduceChannel = make(chan *Task, nReduce)
	c.Files = files
	c.MapperNum = len(files)
	c.ReducerNum = nReduce
	c.AssignPhase = MapPhase
	c.MapWorkerId = 0
	c.ReduceWorkerId = 0
	c.AllMapDone = false
	c.AllReduceDone = false

	//generate map task given input file list
	c.genMapTasks(files)

	//periodically re
	go c.periodicallyRmExpTassks()

	//listen to rpc call
	err := c.server()
	if err != nil{
		log.Println("MakeCoordinator: rpc server error: ", err)
		return nil
	}

	return &c
}


//Generate map tasks and send these task to map channel
func (c *Coordinator) genMapTasks(files []string){
	c.Lock.Lock()
	defer c.Lock.Unlock()

	for _, file := range files{
		task := Task{
			TaskType: MapTask,
			InputFile: file,
			TaskStatus: Ready,
		}

		c.MapChannel <- &task
		log.Pringln("Finish generating map task: ", task)
	}

	log.Pringln("Finish generating all map tasks")
}


func (c *Coordinator) genReduceTasks(){
	c.Lock.Lock()
	defer c.Lock.Unlock()

	for i := 0; i < c.ReducerNum; i++{
		task := Task{
			TaskType: ReduceTask,
			InputFile: fmt.Sprintf("%vmr-*-%v", FinalMapFilePath, i)
			TaskStatus: Ready,
		}

		c.ReduceChannel <- &task
		log.Println("Finish generating reduce task: ", task)
	}

	log.Println("Finish generating all reduce tasks")
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}