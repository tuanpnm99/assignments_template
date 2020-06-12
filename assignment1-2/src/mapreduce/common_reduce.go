package mapreduce

import (
	"bufio"
	"os"
	"sort"
	"encoding/json"
	"strings"
	//"fmt"
)
// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
	countMap := make(map[string][]string)
	var kArr[]string
	for i := 0; i < nMap; i++{

		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, err := os.Open(fileName)
		if err != nil{
			checkError(err)
			return
		}
		
		
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			kv := strings.Split(line, " ")
			value, _ := countMap[kv[0]]
			
			value = append(value, kv[1])
			countMap[kv[0]] = value
		}
		if err := scanner.Err(); err != nil {
			checkError(err)
		}
		
		file.Close()
		//os.Remove(fileName)
	}
	for k, _ := range countMap{
		kArr = append(kArr, k)
		//fmt.Println(k, value)
	}
	sort.Strings(kArr)
	mergeFile, err := os.OpenFile(mergeName(jobName, reduceTaskNumber), os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0666)	
	if err != nil{
		checkError(err)
		return
	}
	defer mergeFile.Close()
	enc := json.NewEncoder(mergeFile)
	for _, key := range kArr{
		value := reduceF(key, countMap[key])
		//fmt.Println(key, value)
		enc.Encode(KeyValue{key, value})
	}
}
