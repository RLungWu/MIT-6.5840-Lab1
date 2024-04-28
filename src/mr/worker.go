package mr

import (
   "encoding/json"
   "fmt"
   "hash/fnv"
   "io"
   "log"
   "net/rpc"
   "os"
   "path/filepath"
   "sort"
   "strconv"
)

type MRWorker struct {
   WorkerId   int                             // WorkerId, initialized by task.MapWorkerId or task.ReduceId
   MapFunc    func(string, string) []KeyValue // Map function, initialized by main function
   ReduceFunc func(string, []string) string   // Reduce function, initialized by main function
   Task       *Task                           // Task, given by coordinator.
   NReduce    int                             // the numer of reduce worker, given by coordinator
   IsDone     bool                            // the variable indicates if all task were done
}

// Initialized the worker
func Worker(map_func func(string, string) []KeyValue, reduce_func func(string, []string) string) {
   worker := MRWorker{
      WorkerId:   -1, // initialized to -1 when the worker isn't handle a task
      MapFunc:    map_func,
      ReduceFunc: reduce_func,
      IsDone:     false,
   }
   log.Println("Initial worker done")
   worker.work()
   log.Println("Worker with WorkerId = ", worker.WorkerId, " finished all works")
}

// Loop working as long as all tasks wern't finished
func (worker *MRWorker) work() {
   for !worker.IsDone {
      task, err := worker.reqTask()
      if err != nil {
         log.Println("work.reqTask err = ", err)
         continue
      }

      if task == nil {
         log.Printf("Worker with WorkerId = %d received all tasks done", worker.WorkerId)
         worker.IsDone = true
         break
      }

      log.Printf("Worker with WorkerId = %d received task = %v", worker.WorkerId, task)
      worker.Task = task

      if task.TaskType == Map {
         worker.WorkerId = task.MapWorkerId
         err := worker.doMap()
         if err != nil {
            log.Println("worker.doMap err = ", err)
            time.Sleep(time.Second)
            continue
         }
         log.Println("Map task done, task = ", task)
      } else {
         worker.WorkerId = task.ReduceWorkerId
         err := worker.doReduce()
         if err != nil {
            log.Println("worker.doReduce err = ", err)
            continue
         }
         log.Println("Reduce task done, task = ", task)
      }
   }
}

// Request task to coordinator
func (worker *MRWorker) reqTask() (*Task, error) {
   args := TaskArgs{}
   reply := TaskReply{}
   err := call("Coordinator.AssignTask", &args, &reply)
   if err != nil {
      log.Println("reqTask.call err = ", err)
      return nil, err
   }
   worker.Task = reply.Task
   if reply.AllDone {
      worker.IsDone = true
      return nil, nil
   }
   worker.NReduce = reply.ReducerNum
   return reply.Task, nil
}

// Execute map tesk details
func (worker *MRWorker) doMap() error {
   task := worker.Task

   intermediate, err := worker.genIntermediate(task.InputFile)
   if err != nil {
      log.Println("doMap.genIntermediate err = ", err)
      return err
   }

   tmp_files, err := worker.writeIntermediateToTmpFiles(intermediate)
   if err != nil {
      log.Println("doMap.writeIntermediateToTmpFiles :", err)
      return err
   }

   err = worker.mapTaskDone(tmp_files)
   if err != nil {
      log.Println("doMap.mapTaskDone err = ", err)
      for _, file := range tmp_files {
         err := os.Remove(file)
         if err != nil {
            log.Println("worker.mapTaskDone.os.Remove err = ", err)
         }
      }
      return err
   }
   return nil
}

// Generate intermediate key-val list
func (worker *MRWorker) genIntermediate(filename string) ([]KeyValue, error) {
   intermediate := make([]KeyValue, 0)
   file, err := os.Open(filename)
   if err != nil {
      log.Println("genIntermediate.os.Open", err)
      return nil, err
   }
   content, err := io.ReadAll(file)
   if err != nil {
      log.Println("genIntermediate.io.ReadAll", err)
      return nil, err
   }
   defer file.Close()

   kva := worker.MapFunc(filename, string(content))
   intermediate = append(intermediate, kva...)
   return intermediate, nil
}

// Write intermediate to map task's temporary files
func (worker *MRWorker) writeIntermediateToTmpFiles(intermediate []KeyValue) ([]string, error) {

   tmp_files := []string{}
   hashed_intermediate := make([][]KeyValue, worker.NReduce)

   for _, kv := range intermediate {
      hash_val := ihash(kv.Key) % worker.NReduce
      hashed_intermediate[hash_val] = append(hashed_intermediate[hash_val], kv)
   }

   for i := 0; i < worker.NReduce; i++ {
      tmp_file, err := os.CreateTemp(TmpMapFilePath, "mr-*.txt")
      if err != nil {
         log.Println("writeIntermediateToTmpFiles.os.CreateTemp err = ", err)
         return nil, err
      }
      defer os.Remove(tmp_file.Name())
      defer tmp_file.Close()

      enc := json.NewEncoder(tmp_file)
      for _, kv := range hashed_intermediate[i] {
         err := enc.Encode(&kv)
         if err != nil {
            log.Println("writeIntermediateToTmpFiles.enc.Encode", err)
            return nil, err
         }
      }

      file_path := fmt.Sprintf("mr-%v-%v", worker.WorkerId, i)
      err = os.Rename(tmp_file.Name(), TmpMapFilePath+file_path)
      if err != nil {
         log.Println("writeIntermediateToTmpFiles os.Rename: ", err)
         return nil, err
      }
      tmp_files = append(tmp_files, TmpMapFilePath+file_path)
   }

   return tmp_files, nil
}

// Report map task done to coordinator
func (worker *MRWorker) mapTaskDone(files []string) error {
   args := MapTaskDoneArgs{
      MapWorkerId: worker.WorkerId,
      Files:       files,
   }
   reply := MapTaskDoneReply{}
   err := call("Coordinator.MapTaskDone", &args, &reply)
   if err != nil {
      log.Println("mapTaskDone.call err = ", err)
      return err
   }
   if reply.Err != nil {
      log.Println("mapTaskDone.reply.Err = ", reply.Err)
      return reply.Err
   }
   return nil
}

// Execute reduce tesk details
func (worker *MRWorker) doReduce() error {

   intermediate, err := worker.collectIntermediate(worker.Task.InputFile)
   if err != nil {
      log.Println("doReduce.collectIntermediate err = ", err)
      return err
   }

   sort.Sort(ByKey(intermediate))

   res := worker.genReduceRes(intermediate)

   tmp_file, err := worker.writeReduceResToTmpFile(res)
   if err != nil {
      log.Println("doReduce.writeReduceResToTmpFile err = ", err)
      return err
   }

   err = worker.reduceTaskDone(tmp_file)
   if err != nil {
      log.Println("doReduce.reduceTaskDone err = ", err)
      err := os.Remove(tmp_file)
      if err != nil {
         log.Println("doReduce.os.Remove err = ", err)
      }
      return err
   }

   return nil
}

// Collect intermediate from different map workers' result files
func (worker *MRWorker) collectIntermediate(file_pattern string) ([]KeyValue, error) {
   intermediate := make([]KeyValue, 0)
   files, err := filepath.Glob(file_pattern)
   if err != nil {
      log.Println("collectIntermediate.filepath.Glob err = ", err)
      return nil, err
   }

   for _, file_path := range files {
      file, err := os.Open(file_path)
      if err != nil {
         log.Println("collectIntermediateos.Open err = ", err)
         return nil, err
      }
      dec := json.NewDecoder(file)
      for {
         var kv KeyValue
         if err := dec.Decode(&kv); err != nil {
            break
         }
         intermediate = append(intermediate, kv)
      }
   }

   return intermediate, nil
}

// Gen reduce result
func (worker *MRWorker) genReduceRes(intermediate []KeyValue) []KeyValue {
   res := make([]KeyValue, 0)
   i := 0
   for i < len(intermediate) {
      //  the key in intermediate [i...j]  is the same since intermediate is already sorted
      j := i + 1
      for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
         j++
      }

      // sum the val number of intermediate [i...j]
      values := []string{}
      for k := i; k < j; k++ {
         values = append(values, intermediate[k].Value)
      }
      output := worker.ReduceFunc(intermediate[i].Key, values)

      kv := KeyValue{Key: intermediate[i].Key, Value: output}
      res = append(res, kv)

      i = j
   }
   return res
}

// write reduce task's result to temporary file
func (worker *MRWorker) writeReduceResToTmpFile(res []KeyValue) (string, error) {
   tempFile, err := os.CreateTemp(TmpReduceFilePath, "mr-") ///home/distributed_system/tmp_res_file/mr-xxxxx(随机字符串)
   if err != nil {
      log.Println("writeReduceResToTmpFile.os.CreateTemp err = ", err)
      return "", err
   }

   // write key-val pair into tmp file
   for _, kv := range res {
      fmt.Fprintf(tempFile, "%s %s\n", kv.Key, kv.Value)
   }

   temp_name := TmpReduceFilePath + "mr-out-" + strconv.Itoa(worker.WorkerId) + ".txt"
   err = os.Rename(tempFile.Name(), temp_name)
   if err != nil {
      log.Println("writeReduceResToTmpFile.os.Rename err = ", err)
      return "", err
   }

   return temp_name, nil
}

// Report reduce task done to coordinator
func (worker *MRWorker) reduceTaskDone(file string) error {
   args := ReduceTaskDoneArgs{
      ReduceWorkerId: worker.WorkerId,
      File:           file,
   }
   reply := ReduceTaskDoneReply{}
   err := call("Coordinator.ReduceTaskDone", &args, &reply)
   if err != nil {
      log.Println("reduceTaskDone.call ", err)
      return err
   }
   if reply.Err != nil {
      log.Println(err)
      return reply.Err
   }
   return nil
}

func ihash(key string) int {
   h := fnv.New32a()
   h.Write([]byte(key))
   return int(h.Sum32() & 0x7fffffff)
}

// rpc call function that send request to coordinator
func call(fun_name string, args interface{}, reply interface{}) error {

   sockname := "/var/tmp/824-mr-"
   sockname += strconv.Itoa(os.Getuid())
   c, err := rpc.DialHTTP("unix", sockname)
   if err != nil {
      log.Fatal("dialing:", err)
      return err
   }
   defer c.Close()

   err = c.Call(fun_name, args, reply)
   if err != nil {
      log.Println("call.call", err)
      return err
   }

   return nil
}