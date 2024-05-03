package mr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	Files       []string
	AssignPhase AssignPhase //Map or Reduce

	MapChannel  chan *Task
	MapperNum   int  //Number of map workers, equal to the input files
	MapWorkerId int  //The counter of map task, start from 0
	AllMapDone  bool //If all map tasks are done

	ReduceChannel  chan *Task
	ReducerNum     int  //Number of reduce workers, can define by the user
	ReduceWorkerId int  //The counter of reduce task, start from 0
	AllReduceDone  bool //If all reduce tasks are done

	Lock sync.Mutex
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("MakeCoordinator")
	c := Coordinator{
		Files:         files,
		AssignPhase:   MapPhase,
		MapChannel:    make(chan *Task, len(files)),
		MapperNum:     len(files),
		MapWorkerId:   0,
		ReduceChannel: make(chan *Task, nReduce),
		ReducerNum:    nReduce,
	}

	//generate map task given input file list
	c.generateMapTask(files)

	go c.periodicallyRmExpireTasks()

	//Listen to rpc call
	err := c.server()
	if err != nil {
		log.Fatal("Coordinator server error: ", err)
		return nil
	}

	return &c
}

func (c *Coordinator) generateMapTask(files []string) {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	for _, file := range files {
		task := Task{
			TaskType:   Map,
			InputFile:  file,
			TaskStatus: Ready,
		}

		c.MapChannel <- &task
		log.Println("Generate map task: ", task)
	}

	log.Println("Finish generating all map task")
}

func (c *Coordinator) periodicallyRmExpireTasks() {
	for !c.Done() {
		time.Sleep(time.Second)
		c.Lock.Lock()
		defer c.Lock.Unlock()
		if c.AssignPhase == MapPhase {
			for i := 0; i < c.MapperNum; i++ {
				task := <-c.MapChannel
				c.MapChannel <- task
				if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime).Seconds() > TaskExpiredTime) {
					log.Println("Map task expired: ", task)
					task.TaskStatus = Ready
				}
			}
		} else { //Reduce phase
			for i := 0; i < c.ReducerNum; i++ {
				task := <-c.ReduceChannel
				c.ReduceChannel <- task
				if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime).Seconds() > TaskExpiredTime) {
					log.Println("Reduce task expired: ", task)
					task.TaskStatus = Ready
				}
			}
		}
	}
}

func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if c.AllMapDone && c.AllReduceDone {
		log.Println("All map and reduce tasks are done")
		time.Sleep(3 * time.Second)
		return true
	}

	return false
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {

	c.Lock.Lock()
	if c.AllMapDone && c.AllReduceDone {
		reply.AllDone = true
		c.Lock.Unlock()
		return nil
	}

	if c.AssignPhase == MapPhase {
		for i := 0; i < c.MapperNum; i++ {
			task := <-c.MapChannel
			c.MapChannel <- task
			if task.TaskStatus == Ready {
				task.MapWorkerId = c.MapWorkerId
				c.MapWorkerId++
				task.TaskStatus = Running
				task.BeginTime = time.Now()
				reply.Task = task
				reply.ReducerNum = c.ReducerNum
				reply.AllDone = false
				log.Println("Distribute map task, task = ", task)
				c.Lock.Unlock()
				return nil
			}
		}
		c.Lock.Unlock()
		if c.ifAllMapDone() {
			c.genReduceTasks()
			c.Lock.Lock()
			c.AssignPhase = ReducePhase
			c.Lock.Unlock()
			err := c.rmMapTmpFiles()
			if err != nil {
				log.Println("AssignTask.rmMapTmpFiles err = ", err)
			}
		}
		log.Println("No map task available")
		return errors.New("No map task available")
	} else {
		for i := 0; i < c.ReducerNum; i++ {
			task := <-c.ReduceChannel
			c.ReduceChannel <- task
			if task.TaskStatus == Ready {
				task.ReduceWorkerId = c.ReduceWorkerId
				c.ReduceWorkerId++
				task.TaskStatus = Running
				task.TaskType = Reduce
				task.BeginTime = time.Now()
				reply.Task = task
				reply.AllDone = false
				log.Println("Distribute reduce task = ", task)
				c.Lock.Unlock()
				return nil
			}
		}
		c.Lock.Unlock()
		if c.ifAllReduceDone() {
			reply.AllDone = true
			err := c.rmMapFinalFiles()
			if err != nil {
				log.Println("AssignTask.rmMapFinalFiles err = ", err)
			}
			err = c.rmReduceTmpFiles()
			if err != nil {
				log.Println("AssignTask.rmMapFinalFiles err = ", err)
			}
			return nil
		}
		log.Println("No reduce task available")

		return errors.New("No reduce task available")
	}

}

// Response to Map task done request from worker
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

// Generate Map task final file
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

// Generate reduce tasks and send these task to map channel
func (c *Coordinator) genReduceTasks() {
	c.Lock.Lock()
	for i := 0; i < c.ReducerNum; i++ {
	   task := Task{
		  TaskType:   Reduce,
		  InputFile:  fmt.Sprintf("%vmr-*-%v", FinalMapFilePath, i),
		  TaskStatus: Ready,
	   }
	   log.Println("Finish generating map task : ", task)
	   c.ReduceChannel <- &task
	}
	c.Lock.Unlock()
	log.Println("Finish generating all reduce tasks")
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

func (c *Coordinator) server() error {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := "/var/tmp/824-mr-"
	sockname += strconv.Itoa(os.Getpid())
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	go http.Serve(l, nil)
	return nil
}
