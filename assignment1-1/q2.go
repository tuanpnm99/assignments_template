package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	result := 0
	for num := range nums {
		result += num
	}
	//fmt.Println("Done calculated in worker with result", result)
	out <- result
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	f, err := os.Open(fileName)
	checkError(err)
	reader := bufio.NewReader(f)
	data, err := readInts(reader)
	checkError(err)
	out := make(chan int)
	in := make(chan int)
	for i := 0; i < num; i++ {
		go sumWorker(in, out)
	}
	//fmt.Println("Launched", num, "workers")
	for _, integer := range data {
		in <- integer
	}
	close(in)
	//fmt.Println("Inputed data")

	result := 0
	for i := 0; i < num; i++ {
		subSum := <-out
		//fmt.Println("Sub sum", subSum)
		result += subSum
	}
	//fmt.Println("Aggregated result", result)

	return result
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
