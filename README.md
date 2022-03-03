# Lab 1: MapReduce



## [MapReduce论文](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)



### 概念

> MapReduce is a programming model and an associated implementation for processing and generating large data sets. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key. Many real world tasks are expressible in this model, as shown in the paper.



> Map, written by the user, takes an input pair and pro- duces a set of intermediate key/value pairs. The MapRe- duce library groups together all intermediate values asso- ciated with the same intermediate key I and passes them to the Reduce function.

> The Reduce function, also written by the user, accepts an intermediate key I and a set of values for that key. It merges together these values to form a possibly smaller set of values. Typically just zero or one output value is produced per Reduce invocation. The intermediate val- ues are supplied to the user’s reduce function via an iter- ator. This allows us to handle lists of values that are too large to fit in memory.



### 应用

- Distributed Grep
- Count of URL Access Frequency
- Reverse Web-Link Graph
- Term-Vector per Host
- Inverted Index
- Distributed Sort



### 工作流程

<img src="note on 6.824.assets/截屏2022-02-24 下午4.32.50.png" alt="截屏2022-02-24 下午4.32.50" style="zoom:50%;" />

1. The MapReduce library in the user program first splits the input files into M pieces of typically 16 megabytes to 64 megabytes (MB) per piece (con- trollable by the user via an optional parameter). It then starts up many copies of the program on a clus- ter of machines.

2. One of the copies of the program is special – **the master**. The rest are workers that are assigned work by the master. There are M map tasks and R reduce tasks to assign. The master picks idle workers and assigns each one a map task or a reduce task.

3. A worker who is assigned a map task reads the contents of the corresponding input split. It parses key/value pairs out of the input data and passes each pair to the user-defined Map function. The intermediate key/value pairs produced by the Map function are **buffered in memory**.

4. **Periodically, the buffered pairs are written to local disk, partitioned into R regions by the partitioning function.** The locations of these buffered pairs on the local disk **are passed back to the master**, **who is responsible for forwarding these locations to the reduce workers**.

5. When a reduce worker is notified by the master about these locations, it uses **remote procedure calls** to read the buffered data from the local disks of the map workers. When a reduce worker has read all intermediate data, it **sorts it by the intermediate keys so that all occurrences of the same key are grouped together**. The sorting is needed because typically many different keys map to the same reduce task. If the amount of intermediate data is **too large to fit in memory**, an **external sort** is used.

6. The reduce worker **iterates over** the sorted intermediate data and for each unique intermediate key encountered, it passes the key and the corresponding set of intermediate values to the user’s Reduce function. The output of the Reduce function is **appended to a final output file** for this reduce partition.

7. When all map tasks and reduce tasks have been completed, the master wakes up the user program. At this point, the MapReduce call in the user program returns back to the user code.



### Master

The master keeps several data structures. **For each map task and reduce task**, it stores the **state** **(idle, in-progress, or completed)**, and the **identity of the worker machine** (for non-idle tasks).

**For each completed map task**, the master stores the **locations and sizes of the R intermediate file regions** produced by the map task.



### Fault Tolerance



#### Worker Failure

**The master pings every worker periodically**. If no re- sponse is received from a worker in a certain amount of time, the master marks the worker as failed. Any map tasks completed by the worker are **reset back to their initial idle state**, and therefore become eligible for schedul- ing on other workers. Similarly, any map task or reduce task in progress on a failed worker is also reset to idle and becomes eligible for rescheduling.

Completed map tasks are re-executed on a failure be- cause their output is stored on the local disk(s) of the failed machine and is therefore inaccessible. Completed reduce tasks do not need to be re-executed since their output is stored in a global file system.

When a map task is executed first by worker A and then later executed by worker B (because A failed), all workers executing reduce tasks are **notified** of the re- execution. **Any reduce task that has not already read the data from worker A will read the data from worker B**.



#### Master Failure

It is easy to make the master write periodic checkpoints of the master data structures described above. If the mas- ter task dies, **a new copy can be started from the last checkpointed state**. 

However, given that there is only a single master, **its failure is unlikely**; therefore our cur- rent implementation aborts the MapReduce computation if the master fails. Clients can check for this condition and retry the MapReduce operation if they desire.



## [Lab](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)



### Your job

Your job is to implement a distributed MapReduce, consisting of two programs, the **coordinator** and the worker. There will be just **one coordinator process**, and **one or more worker processes** executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. The workers will talk to the coordinator via **RPC**. **Each worker process will ask the coordinator for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files.** **The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.**



### A few rules

- The map phase should divide the intermediate keys into buckets for `nReduce` reduce tasks, where `nReduce` is the number of reduce tasks -- argument that `main/mrcoordinator.go` passes to `MakeCoordinator()`. So, each mapper needs to create `nReduce` intermediate files for consumption by the reduce tasks.
- The worker implementation should put the output of the X'th reduce task in the file `mr-out-X`.
- A `mr-out-X` file should contain one line per Reduce function output. The line should be generated with the Go `"%v %v"` format, called with the key and value. Have a look in `main/mrsequential.go` for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.
- You can modify `mr/worker.go`, `mr/coordinator.go`, and `mr/rpc.go`. You can temporarily modify other files for testing, but make sure your code works with the original versions; we'll test with the original versions.
- The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
- `main/mrcoordinator.go` expects `mr/coordinator.go` to implement a `Done()` method that returns true when the MapReduce job is completely finished; at that point, `mrcoordinator.go` will exit.
- When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from `call()`: if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, and so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.



### Hints

- The [Guidance page](https://pdos.csail.mit.edu/6.824/labs/guidance.html) has some tips on developing and debugging.

- One way to get started is to modify `mr/worker.go`'s `Worker()` to send an RPC to the coordinator asking for a task. Then modify the coordinator to respond with the file name of an as-yet-unstarted map task. Then modify the worker to read that file and call the application Map function, as in `mrsequential.go`.

- The application Map and Reduce functions are loaded at run-time using the Go plugin package, from files whose names end in `.so`.

- If you change anything in the `mr/` directory, you will probably have to re-build any MapReduce plugins you use, with something like `go build -race -buildmode=plugin ../mrapps/wc.go`

- This lab relies on the workers sharing a file system. That's straightforward when all workers run on the same machine, but would require a global filesystem like GFS if the workers ran on different machines.

- **A reasonable naming convention for intermediate files is `mr-X-Y`, where X is the Map task number, and Y is the reduce task number.**

- The worker's map task code will need a way to store intermediate key/value pairs in files in a way that can be correctly read back during reduce tasks. **One possibility is to use Go's`encoding/json`package.** To write key/value pairs in JSON format to an open file:

  ```
    enc := json.NewEncoder(file)
    for _, kv := ... {
      err := enc.Encode(&kv)
  ```

  and to read such a file back:

  ```
    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }
      kva = append(kva, kv)
    }
  ```

- The map part of your worker can use the `ihash(key)` function (in `worker.go`) to pick the reduce task for a given key.

- You can steal some code from `mrsequential.go` for reading Map input files, for **sorting intermedate key/value pairs between the Map and Reduce**, and for storing Reduce output in files.

- The coordinator, as an RPC server, will be **concurrent**; don't forget to **lock shared data**.

- Use Go's race detector, with `go build -race` and `go run -race`. `test-mr.sh` by default runs the tests with the race detector.

- Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished. One possibility is for workers to periodically ask the coordinator for work, sleeping with `time.Sleep()` between each request. Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with `time.Sleep()` or `sync.Cond`. Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.

- The coordinator can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. The best you can do is have the coordinator wait for some amount of time, and then give up and re-issue the task to a different worker. For this lab, have the coordinator wait for ten seconds; after that the coordinator should assume the worker has died (of course, it might not have).

- If you choose to implement Backup Tasks (Section 3.6), note that we test that your code doesn't schedule extraneous tasks when workers execute tasks without crashing. Backup tasks should only be scheduled after some relatively long period of time (e.g., 10s).

- To test crash recovery, you can use the `mrapps/crash.go` application plugin. It randomly exits in the Map and Reduce functions.

- To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. **You can use `ioutil.TempFile` to create a temporary file and `os.Rename` to atomically rename it.**

- `test-mr.sh` runs all its processes in the sub-directory `mr-tmp`, so if something goes wrong and you want to look at intermediate or output files, look there. You can temporarily modify `test-mr.sh` to `exit` after the failing test, so the script does not continue testing (and overwrite the output files).

- `test-mr-many.sh` provides a bare-bones script for running `test-mr.sh` with a timeout (which is how we'll test your code). It takes as an argument the number of times to run the tests. You should not run several `test-mr.sh` instances in parallel because the coordinator will reuse the same socket, causing conflicts.

- Go RPC sends only struct fields whose names start with capital letters. Sub-structures must also have capitalized field names.

- When passing a pointer to a reply struct to the RPC system, the object that `*reply` points to should be zero-allocated. The code for RPC calls should always look like

  ```
    reply := SomeType{}
    call(..., &reply)
  ```

  without setting any fields of reply before the call. If you don't follow this requirement, there will be a problem when you pre-initialize a reply field to the non-default value for that datatype, and the server on which the RPC executes sets that reply field to the default value; you will observe that the write doesn't appear to take effect, and that on the caller side, the non-default value remains.









