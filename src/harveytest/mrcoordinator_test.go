package harveytest

import (
	"sync"
	"testing"

	"6.5840/mr"
)

func TestGetTaskHandler(t *testing.T) {
    tests := []struct {
        name       string
        state      string
        mapTasks   []*mr.MapTask
        reduceTasks []*mr.ReduceTask
        expectedType string
        expectedID   int
        expectedFilename string
        expectedNReduce int
    }{
        {
            name: "Map task available",
            state: "map",
            mapTasks: []*mr.MapTask{
                {ID: 1, Status: "notstarted", Filename: "file1"},
            },
            reduceTasks: []*mr.ReduceTask{
				{ID: 1, Status: "notstarted"},
			},
            expectedType: "map",
            expectedID: 1,
            expectedFilename: "file1",
            expectedNReduce: 10,
        },
        {
            name: "All map tasks in progress",
            state: "map",
            mapTasks: []*mr.MapTask{
                {ID: 1, Status: "inprogress", Filename: "file1"},
				{ID: 2, Status: "completed", Filename: "file2"},
            },
            reduceTasks: []*mr.ReduceTask{
				{ID: 1, Status: "notstarted"},
			},
            expectedType: "wait",
			expectedID: 0,
			expectedFilename: "",
			expectedNReduce: 0,
        },
        {
            name: "Reduce task available",
            state: "reduce",
            mapTasks: []*mr.MapTask{
				{ID: 1, Status: "completed", Filename: "file1"},
				{ID: 2, Status: "completed", Filename: "file2"},
			},
            reduceTasks: []*mr.ReduceTask{
                {ID: 1, Status: "inprogress"},
				{ID: 2, Status: "notstarted"},
            },
            expectedType: "reduce",
            expectedID: 2,
			expectedFilename: "",
			expectedNReduce: 0,
        },
        {
            name: "All reduce tasks in progress",
            state: "reduce",
            mapTasks: []*mr.MapTask{
				{ID: 1, Status: "completed", Filename: "file1"},
				{ID: 2, Status: "completed", Filename: "file2"},
			},
            reduceTasks: []*mr.ReduceTask{
                {ID: 1, Status: "inprogress"},
				{ID: 2, Status: "inprogress"},
				{ID: 3, Status: "completed"},
            },
            expectedType: "wait",
			expectedID: 0,
			expectedFilename: "",
			expectedNReduce: 0,
        },
        {
            name: "All tasks completed",
            state: "completed",
            mapTasks: []*mr.MapTask{
				{ID: 1, Status: "completed", Filename: "file1"},
				{ID: 2, Status: "completed", Filename: "file2"},
			},
            reduceTasks: []*mr.ReduceTask{
                {ID: 1, Status: "completed"},
				{ID: 2, Status: "completed"},
				{ID: 3, Status: "completed"},
            },
            expectedType: "shutdown",
			expectedID: 0,
			expectedFilename: "",
			expectedNReduce: 0,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            coordinator := mockCoordinator(tt.state, tt.mapTasks, tt.reduceTasks)
            args := &mr.GetTaskArgs{}
            reply := &mr.GetTaskReply{}
            err := coordinator.GetTaskHandler(args, reply)
            if err != nil {
                t.Fatalf("GetTaskHandler returned error: %v", err)
            }
            if reply.TaskType != tt.expectedType {
                t.Errorf("expected TaskType %s, got %s", tt.expectedType, reply.TaskType)
            }
            if reply.TaskID != tt.expectedID {
                t.Errorf("expected TaskID %d, got %d", tt.expectedID, reply.TaskID)
            }
            if reply.Filename != tt.expectedFilename {
                t.Errorf("expected Filename %s, got %s", tt.expectedFilename, reply.Filename)
            }
            if reply.NReduce != tt.expectedNReduce {
                t.Errorf("expected NReduce %d, got %d", tt.expectedNReduce, reply.NReduce)
            }
        })
    }
}

func TestGetTaskHandlerConcurrency(t *testing.T) {
    coordinator := mockCoordinator("map", []*mr.MapTask{
        {ID: 1, Status: "notstarted", Filename: "pg-1.txt"},
        {ID: 2, Status: "notstarted", Filename: "pg-2.txt"},
    }, []*mr.ReduceTask{})

    var wg sync.WaitGroup
    numGoroutines := 4
    results := make(chan *mr.GetTaskReply, numGoroutines)

    for i := 0; i < numGoroutines; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            args := &mr.GetTaskArgs{}
            reply := &mr.GetTaskReply{}
            err := coordinator.GetTaskHandler(args, reply)
            if err != nil {
                t.Errorf("GetTaskHandler returned error: %v", err)
            }
            results <- reply
        }(i)
    }

    wg.Wait()
    close(results)

    expectedReplies := []*mr.GetTaskReply {
        {TaskType: "map", TaskID: 1, Filename: "pg-1.txt", NReduce: 10},
		{TaskType: "map", TaskID: 2, Filename: "pg-2.txt", NReduce: 10},
		{TaskType: "wait", TaskID: 0, Filename: "", NReduce: 0},
        {TaskType: "wait", TaskID: 0, Filename: "", NReduce: 0},
    }

    var resultsSlice []*mr.GetTaskReply
    for reply := range results {
        resultsSlice = append(resultsSlice, reply)
    }
    if !containsExactlyInAnyOrder(resultsSlice, expectedReplies) {
		t.Log(expectedReplies)
		t.Log(results)
		t.Errorf("expected Replies %+v, got %+v", expectedReplies, results)
	}

	coordinator.Mu.Lock()
	defer coordinator.Mu.Unlock()

	expectedMapTasks := []mr.MapTask {
		{ID: 1, Status: "inprogress", Filename: "pg-1.txt"},
        {ID: 2, Status: "inprogress", Filename: "pg-2.txt"},
	}
	
	for i, task := range coordinator.MapTasks {
		if task.ID != expectedMapTasks[i].ID {
            t.Errorf("expected TaskID %d, got %d", expectedMapTasks[i].ID, task.ID)
        }
        if task.Status != expectedMapTasks[i].Status {
            t.Errorf("expected Filename %s, got %s", expectedMapTasks[i].Status, task.Status)
        }
        if task.Filename != expectedMapTasks[i].Filename {
            t.Errorf("expected NReduce %s, got %s", expectedMapTasks[i].Filename, task.Filename)
        }
	}
}

func TestCreateMapTasks(t *testing.T) {
    files := []string{"file1.txt", "file2.txt", "file3.txt"}
    expectedTasks := []*mr.MapTask{
        {ID: 0, Status: "notstarted", Filename: "file1.txt"},
        {ID: 1, Status: "notstarted", Filename: "file2.txt"},
        {ID: 2, Status: "notstarted", Filename: "file3.txt"},
    }

    tasks := mr.CreateMapTasks(files)

    if len(tasks) != len(expectedTasks) {
        t.Fatalf("expected %d tasks, got %d", len(expectedTasks), len(tasks))
    }

    for i, task := range tasks {
        if task.ID != expectedTasks[i].ID {
            t.Errorf("expected task ID %d, got %d", expectedTasks[i].ID, task.ID)
        }
        if task.Status != expectedTasks[i].Status {
            t.Errorf("expected task Status %s, got %s", expectedTasks[i].Status, task.Status)
        }
        if task.Filename != expectedTasks[i].Filename {
            t.Errorf("expected task Filename %s, got %s", expectedTasks[i].Filename, task.Filename)
        }
    }
}

// helpers
func mockCoordinator(state string, mapTasks []*mr.MapTask, reduceTasks []*mr.ReduceTask) *mr.Coordinator {
    return &mr.Coordinator{
        MapTasks:    mapTasks,
        ReduceTasks: reduceTasks,
        NReduce:     10,
        State:       state,
    }
}

func containsExactlyInAnyOrder(a, b []*mr.GetTaskReply) bool {
    if len(a) != len(b) {
        return false
    }

    counts := make(map[mr.GetTaskReply]int)
    for _, reply := range a {
        counts[*reply]++
    }
    for _, reply := range b {
        if counts[*reply] == 0 {
            return false
        }
        counts[*reply]--
    }
    return true
}
