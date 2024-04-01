package poorq

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/subiz/executor/v2"
	"github.com/subiz/log"
)

var allQueues = []*Queue{}

func init() {
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			for _, queue := range allQueues {
				queue.status()
			}
		}
	}()
}

const NTHREAD = 10

type Queue struct {
	*sync.Mutex

	tail int64

	path string

	isRunning bool // sleep | running

	cb    func(data []byte)
	tasks []Task
}

type Task struct {
	Id   int64
	Data []byte
}

func NewQueue(path string, cb func(data []byte)) *Queue {
	if path == "" {
		path = "."
	}
	if path[len(path)-1] != '/' {
		path += "/"
	}
	queue := &Queue{Mutex: &sync.Mutex{}, path: path, cb: cb}
	tasks := ListTaskFromDisk(path)

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

func (queue *Queue) status() {
	queue.Lock()
	fmt.Println("QUEUE REPORT STATUS", queue.path, "PENDING TAKS:", len(queue.tasks), "TAIL:", queue.tail, "ISRUNNING", queue.isRunning)
	queue.Unlock()
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
		executor.Async(len(tasks), func(i int, _ *sync.Mutex) {
			defer func() {
				if r := recover(); r != nil {
					log.EServer(nil, log.M{"r": r, "path": queue.path})
				}
			}()
			queue.cb(tasks[i].Data)
			CommitTaskToDisk(queue.path, tasks[i].Id)
		}, NTHREAD)
		queue.Lock()
	}
	queue.isRunning = false
	queue.Unlock()
}

func (queue *Queue) Push(data []byte) (int64, error) {
	queue.Lock()
	defer queue.Unlock()
	task_id := queue.tail + 1
	if err := SaveTaskToDisk(queue.path, task_id, data); err != nil {
		return -1, err
	}
	queue.tasks = append(queue.tasks, Task{Id: task_id, Data: data})

	queue.tail = queue.tail + 1
	if !queue.isRunning {
		go queue.do()
	}
	return queue.tail, nil
}

func ListTaskFromDisk(path string) []Task {
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

func SaveTaskToDisk(path string, id int64, data []byte) error {
	file := fmt.Sprintf("%s%d", path, id)
	return os.WriteFile(file, data, 0644)
}

func CommitTaskToDisk(path string, id int64) error {
	return os.Remove(fmt.Sprintf("%s%d", path, id))
}
