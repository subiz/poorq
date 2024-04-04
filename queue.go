package poorq

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/subiz/log"
)

var allQueues = []*Queue{}

func init() {
	go func() {
		for {
			time.Sleep(2 * time.Minute)
			for _, queue := range allQueues {
				// clean runningTasks so the memory doesn't growth too big
				runningTasks := map[int64]int64{}
				queue.Lock()
				slowJobs := map[int64]int64{} // id -> duration, records which job took looong time to execute
				now := time.Now().UnixMilli()
				for jobid, start := range queue.runningTasks {
					runningTasks[jobid] = start
					if now-start > 60_000 {
						slowJobs[jobid] = now - start/1000
					}
				}
				queue.runningTasks = runningTasks

				lentask, tail, isRunning := len(queue.tasks), queue.tail, queue.isRunning
				queue.Unlock()

				log.Info("POORQ REPORT STATUS", queue.path, "PENDING TAKS:", lentask, "TAIL:", tail, "ISRUNNING", isRunning)
				var slowjobs string
				for id, sec := range slowJobs {
					slowjobs += strconv.Itoa(int(id)) + ":" + strconv.Itoa(int(sec)) + " "
				}
				if slowjobs != "" {
					log.Info("POORQ REPORT SLOWJOB", slowjobs)
				}
			}
		}
	}()
}

type Queue struct {
	*sync.Mutex

	tail      int64
	path      string
	isRunning bool // sleep | running
	cb        func(data []byte) error
	tasks     []Task

	runningTasks map[int64]int64 // id -> timestamp
}

type Task struct {
	Id   int64
	Data []byte
}

// NewQueue creates new queue, do not call this function multiple times with the same <path>
func NewQueue(path string, cb func(data []byte) error) *Queue {
	if path == "" {
		path = "."
	}
	if path[len(path)-1] != '/' {
		path += "/"
	}
	queue := &Queue{Mutex: &sync.Mutex{}, path: path, cb: cb, runningTasks: map[int64]int64{}}
	tasks := listTaskFromDisk(path)

	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Id < tasks[j].Id
	})
	queue.tasks = tasks
	if len(tasks) > 0 {
		queue.tail = tasks[len(tasks)-1].Id
	}
	allQueues = append(allQueues, queue)
	go queue.do()
	return queue
}

func (queue *Queue) do() {
	queue.Lock()
	if queue.isRunning {
		queue.Unlock() // quit if already run
		return
	}
	queue.isRunning = true

	for len(queue.tasks) > 0 {
		tasks := queue.tasks
		queue.tasks = nil
		queue.Unlock()
		for _, task := range tasks {
			go func(task Task) {
				queue.Lock()
				start := time.Now()
				queue.runningTasks[task.Id] = start.UnixMilli()
				queue.Unlock()

				var err error
				defer func() {
					commitTaskToDisk(queue.path, task.Id)
					queue.Lock()
					delete(queue.runningTasks, task.Id)
					queue.Unlock()

					if r := recover(); r != nil {
						err = log.EServer(nil, log.M{"r": r, "path": queue.path, "task_id": task.Id})
					}

					status := "success"
					if err != nil {
						status = "failed, " + err.Error()
					}

					log.Info("subiz", "POORQ", queue.path, "TASKDONE", task.Id, "TOOK", time.Since(start), "STATUS", status)
				}()
				err = queue.cb(task.Data)
			}(task)
		}
		queue.Lock()
	}
	queue.isRunning = false
	queue.Unlock()
}

// Push adds a new task the the queue
func (queue *Queue) Push(data []byte) (int64, error) {
	queue.Lock()
	defer queue.Unlock()
	task_id := queue.tail + 1
	if err := saveTaskToDisk(queue.path, task_id, data); err != nil {
		return -1, err
	}
	queue.tasks = append(queue.tasks, Task{Id: task_id, Data: data})

	queue.tail = queue.tail + 1
	if !queue.isRunning {
		go queue.do()
	}
	return queue.tail, nil
}

func listTaskFromDisk(path string) []Task {
	files, err := os.ReadDir(path)
	if err != nil {
		log.EServer(err, log.M{"path": path})
		return nil
	}

	var tasks []Task
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filename := fmt.Sprintf("%s%s", path, file.Name())
		dat, err := os.ReadFile(filename)
		if err != nil {
			log.EServer(err, log.M{"path": path})
		}
		task_id, _ := strconv.Atoi(file.Name())
		tasks = append(tasks, Task{Id: int64(task_id), Data: dat})
	}
	return tasks
}

func saveTaskToDisk(path string, id int64, data []byte) error {
	file := fmt.Sprintf("%s%d", path, id)
	return os.WriteFile(file, data, 0644)
}

func commitTaskToDisk(path string, id int64) error {
	return os.Remove(fmt.Sprintf("%s%d", path, id))
}
