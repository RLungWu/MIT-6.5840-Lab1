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


//Periodically remove expired tasks and reset them into ready status
func (c *Coordinator) peroidocallyRmExpTasks(){
	for !c.Done(){
		time.Sleep(time.second)
		c.Lock.Lock()
		if c.AssignPhase == MapPhase{
			for i := 0; i < c.MapperNum; i++{
				task := <- c.MapChannel
				c.MapChannel <- task
				if task.TaskStatus == Running &&  (time.Now().Sub(task.BeginTime) > TaskExpiredTime * time.Second){
					task.Taskstatus = Ready
					log.Pringf("Task with MapWorker id = %d is expired", task.MapWorkerId)
				}
			}
		}else{
			for i := 0; i < c.ReducerNum; i++{
				task := c.ReduceChannel
				c.ReduceChannel <- task
				if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime) > TaskExpiredTime * time.Second){
					task.TaskStatus = Ready
					log.Pringf("Task with ReduceWorker id = %d is expired", task.ReduceWorkerId)
				}
			}
		}

		c.Lock.Unlock()
	}
}

func (c *Coordinaotr) AssignTask(args *TaskArgs, reply *TaskReply) error{
	c.Lock.Lock()
	
	if c.AllMapDone && c.AllReduceDone{
		reply.AllDone = tmp_reduce_file
		c.Lock.Unlock()
		return nil
	}

	if c.AssignPhase == MapPhase{
		for i := 0; i < c.MapperNum; i++{
			task := <- c.MapChannel
			c.MapChannel <- task
			
			if task.TaskStatus == Ready{
				task.MapWorkerId = c.MapWorkerId
				c.MapWorkerId++
				task.TaskStatus = Running
				task.BeginTime = time.Now()
				reply.Task = task
				reply.ReducerNum = c.ReducerNum
				reply.AllDone = false
				log.Println("Assign map task: ", task)
				c.Lock.Unlock()
				return nil
			}
		}

		c.Lock.Unlock()

		if c.ifAllMapDone(){
			c.genReduceTasks()
			c.Lock.Lock()
			c.AssighPhase = ReducePhase
			c.Lock.Unlock()
			
			err := c.rmMapTmpFiles()
			if err != nil{
				log.Println("AssignTask: remove map tmp files error: ", err)
			}
		}
		log.Pringln("No map task to assign")
		return errors.New("No map task to assign")
	}else{ //c.AssignPhase == ReducePhase
		for i := 0; i < c.ReducerNum; i++{
			task := <- c.ReduceChannel
			c.ReduceChannel <- task
			
			if task.TaskStatus == Ready{
				task.ReduceWorkerId = c.ReduceWorkerId
				c.ReduceWorkerId++
				task.TaskStatus = Running
				task.BeginTime = time.Now()
				reply.Task = task
				reply.AllDone = false
				log.Println("Assign reduce task: ", task)
				c.Lock.Unlock()
				return nil
			}
		}

		c.Lock.Unlock()
		if c.ifAllReduceDone(){
			reply.AllDone = true
			err := c.rmMapFinalFiles()
			if err != nil{
				log.Println("AssignTask: remove map final files error: ", err)
			}
			err := c.rmReduceTmpFiles()
			if err != nil{
				log.Println("AssignTask: remove reduce tmp files error: ", err)
			}
			return nil
		}

		log.Println("No reduce task to assign")
		return errors.New("No reduce task to assign")
	}
}

func (c *Coordinator) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	c.Lock.Lock()
	if c.AllMapDone {
	   c.Lock.Unlock()
	   return errors.New("All map done")
	}
 
	for i := 0; i < c.MapperNum; i++ {
	   task := <-c.MapChannel
	   c.MapChannel <- task
	   if task.TaskStatus == Running && time.Now().Sub(task.BeginTime) > (TaskExpiredTime)*time.Second {
		  task.TaskStatus = Ready
		  log.Printf("Map task with MapWorkerId = %d expired", task.MapWorkerId)
		  continue
	   }
 
	   if args.MapWorkerId == task.MapWorkerId && task.TaskStatus == Running && time.Now().Sub(task.BeginTime) <= TaskExpiredTime*time.Second {
		  task.TaskStatus = Finished
		  err := c.genMapFinalFiles(args.Files)
		  if err != nil {
			 task.TaskStatus = Ready
			 reply.Err = err
			 log.Println("MapTaskDone.genMapFinalFiles err = ", err)
			 c.Lock.Unlock()
			 return err
		  }
		  log.Printf("Map task with MapWorkerId = %d is finished in time", task.MapWorkerId)
		  c.Lock.Unlock()
		  return nil
	   }
	}
	c.Lock.Unlock()
 
	reply.Err = errors.New(fmt.Sprintf("Map task with MapWorkerId = %d cannot be done", args.MapWorkerId))
	log.Println(fmt.Sprintf("Map task with MapWorkerId = %d cannot be done", args.MapWorkerId))
	return errors.New(fmt.Sprintf("Map task with MapWorkerId = %d cannot be done", args.MapWorkerId))
 }

func (c *Coordinator) genMapFinalFiles(files []string) error {
	for _, file := range files {
	   tmp_file, err := os.Open(file)
	   if err != nil {
		  log.Println("genMapFinalFiles err = ", err)
		  return err
	   }
	   defer tmp_file.Close()
	   tmp_file_name := filepath.Base(file)
	   final_file_path := FinalMapFilePath + tmp_file_name
	   final_file, err := os.Create(final_file_path)
	   if err != nil {
		  log.Println("genMapFinalFiles.os.Create err = ", err)
		  return err
	   }
	   defer final_file.Close()
	   _, err = io.Copy(final_file, tmp_file)
	   if err != nil {
		  log.Println("genMapFinalFiles.io.Copy err = ", err)
		  return err
	   }
	}
	return nil
}

 // Response to reduce task done request from worker
func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {

	c.Lock.Lock()
	if c.AllReduceDone {
	   log.Println("All reduce task done")
	   c.Lock.Unlock()
	   return errors.New("All reduce task done")
	}
 
	for i := 0; i < c.ReducerNum; i++ {
	   task := <-c.ReduceChannel
	   c.ReduceChannel <- task
	   if task.TaskStatus == Running && time.Now().Sub(task.BeginTime) > (TaskExpiredTime)*time.Second {
		  task.TaskStatus = Ready
		  log.Printf("Reduce task with ReduceWorkerId = %d expired", task.ReduceWorkerId)
		  continue
	   }
	   if args.ReduceWorkerId == task.ReduceWorkerId && task.TaskStatus == Running && time.Now().Sub(task.BeginTime) <= (TaskExpiredTime)*time.Second {
		  task.TaskStatus = Finished
		  err := c.genReduceFinalFile(args.File)
		  if err != nil {
			 log.Println("ReduceTaskDone.genReduceFinalFile err = ", err)
			 task.TaskStatus = Ready
			 reply.Err = err
			 c.Lock.Unlock()
			 return err
		  }
		  log.Printf("Reduce task with ReduceWorkerId = %d is finished in time", task.ReduceWorkerId)
		  c.Lock.Unlock()
		  return nil
 
	   }
	}
	c.Lock.Unlock()
 
	reply.Err = errors.New(fmt.Sprintf("Reduce task with ReduceWorkerId = %d cannot be done", args.ReduceWorkerId))
	log.Println(fmt.Sprintf("Reduce task with ReduceWorkerId = %d cannot be done", args.ReduceWorkerId))
	return errors.New(fmt.Sprintf("Reduce task with ReduceWorkerId = %d cannot be done", args.ReduceWorkerId))
 }
 
 // Generate Reduce task final file
 func (c *Coordinator) genReduceFinalFile(file string) error {
	tmp_file, err := os.Open(file)
	if err != nil {
	   log.Println("genReduceFinalFile.os.Open err = ", err)
	   return err
	}
	defer tmp_file.Close()
	tmp_file_name := filepath.Base(file)
	final_file_path := FinalReduceFilePath + tmp_file_name
	final_file, err := os.Create(final_file_path)
	if err != nil {
	   log.Println("genReduceFinalFile.os.Create err = ", err)
	   return err
	}
	defer final_file.Close()
	_, err = io.Copy(final_file, tmp_file)
	if err != nil {
	   log.Println("genReduceFinalFile.os.Copy err = ", err)
	   return err
	}
	return nil
 }
 
 // Remove map task's  temporary files
 func (c *Coordinator) rmMapTmpFiles() error {
	d, err := os.Open(TmpMapFilePath)
	if err != nil {
	   log.Println("rmMapTmpFiles os.Open err = ", err)
	   return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
	   log.Println("rmMapTmpFiles.d.Readdirnames err = ", err)
	   return err
	}
	for _, name := range names {
	   err = os.RemoveAll(TmpMapFilePath + name)
	   if err != nil {
		  log.Println("rmMapTmpFiles.os.RemoveAll err = ", err)
		  return err
	   }
	}
	return nil
 }
 
 // Remove map task's final files
 func (c *Coordinator) rmMapFinalFiles() error {
	d, err := os.Open(FinalMapFilePath)
	if err != nil {
	   log.Println("rmMapFinalFiles.os.Open err = ", err)
	   return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
	   log.Println("rmMapFinalFiles.d.Readdirnames err = ", err)
	   return err
	}
	for _, name := range names {
	   err = os.RemoveAll(FinalMapFilePath + name)
	   if err != nil {
		  log.Println("rmMapFinalFiles.os.RemoveAll err = ", err)
		  return err
	   }
	}
	return nil
 }
 
// Remove reduce task's temporary files
func (c *Coordinator) rmReduceTmpFiles() error {
	d, err := os.Open(TmpReduceFilePath)
	if err != nil {
	   log.Println("rmReduceTmpFiles.os.Open err = ", err)
	   return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
	   log.Println("rmReduceTmpFiles.d.Readdirnames err = ", err)
	   return err
	}
	for _, name := range names {
	   err = os.RemoveAll(TmpReduceFilePath + name)
	   if err != nil {
		  log.Println("rmReduceTmpFiles.os.RemoveAll err = ", err)
		  return err
	   }
	}
	return nil
}
 
// Vist every task in map channel and find out if all map task were done
func (c *Coordinator) ifAllMapDone() bool {
	c.Lock.Lock()
	for i := 0; i < c.MapperNum; i++ {
	   task := <-c.MapChannel
	   if task.TaskStatus != Finished {
		  c.MapChannel <- task
		  c.Lock.Unlock()
		  return false
	   }
	   c.MapChannel <- task
	}
	c.AllMapDone = true
	c.Lock.Unlock()
	return true
}
 
// Vist every task in reduce channel and find out if all reduce task were done
func (c *Coordinator) ifAllReduceDone() bool {
	c.Lock.Lock()
	for i := 0; i < c.ReducerNum; i++ {
	   task := <-c.ReduceChannel
	   c.ReduceChannel <- task
	   if task.TaskStatus != Finished {
		  c.Lock.Unlock()
		  return false
	   }
	}
	c.AllReduceDone = true
	c.Lock.Unlock()
	return true
}
 

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()


	//l, e := net.Listen("tcp", ":1234")
	sockname := "/var/tmp/824-mr-"
	sockname += strconv.Itoa(os.Getuid())
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
	c.Lock.Lock()
	if c.AllMapDone && c.AllReduceDone{
		c.Lock.Unlock()
		log.Println("All tasks done!")
		time.Sleep(3 * time.Second) //Wait for worker to exit the program before the main end
		return true
	}
	c.Lock.Unlock()
	return false
}