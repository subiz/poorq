# PoorQ
Poorly designed but dead simple persistent task queue

## Design

* Slow
* Single point of failure
* Safe
* At least one delivery
* **Maintenance-free**
* **Bug-free**
* Dont push a million tasks to it
* **Should run same process with the publisher and consumer**
* Each pending task is a file on the disk
* After a consumer commits a task, the corresponding file will be deleted from the disk
* When the queue start, it loads all files on the disk to see which tasks is pending
