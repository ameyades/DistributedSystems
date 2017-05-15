package mapreduce

import(
	"fmt"
	//"time"
	"sync"
) 

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
		//data race on i? there's literally no way to do this...
		for i := 0; i < ntasks; i++ {
			fmt.Printf("-------------------------------------------------i: %v\n", i)	
        	wg.Add(1)
        	//c := make(chan int)  // Allocate a channel.
        	go func(i int, registerChan chan string) {       		
        		var try2 string
        		fmt.Printf("MAP before register and call with i val: %v\n", i)
        		//also data race on this channel
        		s := <- registerChan
        		firstTry := false
        		//data race on i?
				args := DoTaskArgs{JobName: jobName, File: mapFiles[i], Phase: phase, TaskNumber: i, NumOtherPhase: ntasks}
        		success := call(s, "Worker.DoTask", args, nil)
        		if success == true {
        			firstTry = true
        			fmt.Printf("MAP call succeeded, with worker: %s, with i val: %v\n", s, i)
        		}
        		for success == false {
        			try2 = ""
        			try2 = <- registerChan
        			success = call(try2, "Worker.DoTask", args, nil)
        		}
        		if firstTry == true {
        			fmt.Printf("MAP call succeeded, with worker: %s, with i val: %v\n", try2, i)
        		}
        		wg.Done()
        		registerChan <- try2
        		/*if success == true {
        			fmt.Printf("MAP call succeeded, with worker: %s, with i val: %v\n", s, i)
        			wg.Done()
        		} else {
        			fmt.Printf("MAP call failed, with i val: %v\n", i)
        			try2 = <- registerChan
        			fmt.Printf("MAP try2, got worker: %s with i val: %v\n", try2, i)
        			success = call(try2, "Worker.DoTask", args, nil)
        			if success == true {
        				fmt.Printf("MAP second try call succeeded, with i val: %v\n", i)
        			} else {
        				fmt.Printf("MAP second try call failed, with i val: %v\n", i)
        			}
        			wg.Done()
        			registerChan <- try2
        		}*/	
        		if firstTry == true {
        			registerChan <- s	
        		}        		//c <- 1  // Send a signal; value does not matter.	
        	}(i, registerChan)	
        	//<-c 
        	fmt.Printf("MAP after FUNC with i val: %v\n", i)
		}
		wg.Wait()
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		fmt.Printf("In reduce phase\n")
		for i := 0; i < ntasks; i++ {
			fmt.Printf("-------------------------------------------------i: %v\n", i)	
        	wg.Add(1)
        	go func(i int, registerChan chan string) {
        		var try2 string
        		fmt.Printf("REDUCE before register and call with i val: %v\n", i)
        		s := <- registerChan
        		firstTry := false
				args := DoTaskArgs{JobName: jobName, File: "", Phase: phase, TaskNumber: i, NumOtherPhase: ntasks}
        		success := call(s, "Worker.DoTask", args, nil)
        		if success == true {
        			firstTry = true
        			fmt.Printf("MAP call succeeded, with worker: %s, with i val: %v\n", s, i)
        		}
        		for success == false {
        			try2 = ""
        			try2 = <- registerChan
        			success = call(try2, "Worker.DoTask", args, nil)
        		}
        		if firstTry == true {
        			fmt.Printf("MAP call succeeded, with worker: %s, with i val: %v\n", try2, i)
        		}
        		wg.Done()
        		registerChan <- try2
        		/*
        		//return something from the call?
        		if success == true {
        			fmt.Printf("REDUCE call succeeded, with worker: %s, with i val: %v\n", s, i)
        			wg.Done() 
        		} else {
        			fmt.Printf("REDUCE call failed, with i val: %v\n", i)
        			try2 = <- registerChan
        			fmt.Printf("REDUCE try2, got worker: %s with i val: %v\n", try2, i)
        			success = call(try2, "Worker.DoTask", args, nil)
        			if success == true {
        				fmt.Printf("REDUCE second try call succeeded, with i val: %v\n", i)
        			} else {
        				fmt.Printf("REDUCE second try call failed, with i val: %v\n", i)
        			}
        			wg.Done() 
        			registerChan <- try2
        		}	*/
        		fmt.Printf("REDUCE after call with i val: %v\n", i)
        		if firstTry == true {
        			registerChan <- s
        		}
        	}(i, registerChan)
        	fmt.Printf("REDUCE after FUNC with i val: %v\n", i)
		}
		wg.Wait()
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	fmt.Printf("ntasks: %v, n_other: %v\n", ntasks, n_other)
	//var wg sync.WaitGroup
	fmt.Printf("registerChan: \n %v \n", registerChan)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
