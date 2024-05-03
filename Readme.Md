# MIT 6.5840 Lab 1 MapReduce

## What is MapReduce?
According to the MapReduce Paper, MapReduce is a model.
The users only need to know how to write the Map function and Reduce Function, and the model can handle another, just like writting the program on the non-distributed function.    
![Map Reduce Model](https://github.com/RLungWu/MIT-6.5840-lab1/blob/main/img/MapReduce.png)

## Reference
Paper: https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf
Lab1: https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

## What did I do?
I complete the program in src/mr files. This file contains three program: coordinator.go, rpc.go and worker.go.

### rpc.go
In the rpc.go, I defined different kinds of datatype which help me implement the following code.

### coordinator.go
For each function:
1. MakeCoordinator: Initial Master(coordinator). I use two channel, map task and reduce task to mantain. The number of mapper is decided by the number of file, and the number of reducer is designed by the "user program", which is 6. 
2. periodicallyRmExpTasks: This function is used to check whether the program is expred. Use the Map task channel and Reduce task channel to check the different task. 
3. AssignTask: If the worker ask for a task, the workerId in master would add 1, which to avoid the same task to be apply to different worker.
4. MapTaskDone and ReduceTaskDone: Looping the task in channel, and check wheter there is some task could be assigned to finished.
5. genMapTasks, genReduceTasks : generate tasks. GenReduceTasks would only run after the Map tasks is done.
6. genMapFinalFiles, genReduceFinalFiles,rmMapTmpFiles and rmReduceTmpFiles: use to collec the data after the worker finished.
7. ifAllMapDone, ifAllReduceDone: checked the work is done.
8. Done(): help the user program to quit the program.
### worker.go
For each function:
1. Worker: Initial worker. if a worker didn't get the task, it's workerId is -1.
2. work: If the worker didn't receive the message of Done, the worker will request a task.
3. doMap: Do the Map task. Read file and generate intermediate(key=word, val=1). and split intermediate into different file. 
4. doReduce: Do the reduce task. collect the intermediate and sort the intermediate. then sum the val of  same key in intermedlate.
5. call: Connect with master.

