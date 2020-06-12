package mapreduce

import (
	"sync"
)
// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	var sync sync.WaitGroup
	for i := 0; i < ntasks; i++{
		debug("Schedule: %v %v task (%d I/Os)\n", i, phase, nios)
		sync.Add(1)
		go func(i int){
			defer sync.Done()
			arg := DoTaskArgs{
				JobName: mr.jobName, 
				File: mr.files[i],
				Phase: phase,
				TaskNumber: i,
				NumOtherPhase: nios}
			worker := <- mr.registerChannel
			for !call(worker, "Worker.DoTask", &arg, new(struct{})){
				worker = <- mr.registerChannel
			}
			//critital to launch a new go rountine because it will block 
			//when we put the worker back to the channel	
			go func(){
				
				mr.registerChannel <- worker
			}()
		}(i)
	}
	sync.Wait()

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	debug("Schedule: %v phase done\n", phase)
}
