package poorq

import (
	"fmt"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	queue := NewQueue("./test_sample/", func(dat []byte) {
		fmt.Println("DOING", string(dat))
		time.Sleep(4 * time.Second)
	})

	var tail int64
	var err error
	tail, err = queue.Push([]byte("xin chao"))
	if tail != 1 {
		t.Errorf("should be 1 got %d", tail)
	}
	if err != nil {
		t.Errorf("should not err %v", err)
	}
	tail, err = queue.Push([]byte("hello"))
	if tail != 2 {
		t.Errorf("should be 1 got %d", tail)
	}
	if err != nil {
		t.Errorf("should not err %v", err)
	}

	time.Sleep(10 * time.Second)
}
