package raft

// import "log"
import "time"
import "fmt"

// Debugging
const Debug = false

var debugStart time.Time = time.Now()




func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		// log.Printf(format, a...)
		// fmt.Printf(time.Now().String() + format + "\n", a...)
		time := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%06d %v", time, format)
		fmt.Printf(prefix + "\n", a...)
	}
	return
}
