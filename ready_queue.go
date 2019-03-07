package delayqueue

import (
	"fmt"
)

// 添加JobId到队列中
func pushToReadyQueue(queueName string, jobId string) error {
	queueName = fmt.Sprintf(Setting.QueueName, queueName)
	_, err := execRedisCommand("RPUSH", queueName, jobId)

	return err
}


// 从队列中阻塞获取JobId
func blockPopFromReadyQueue(queue string, timeout int) (string, error) {


	value, err := execRedisCommand("BLPOP", queue,timeout)
	if err != nil {
		return "", err
	}
	if value == nil {
		return "", nil
	}
	var valueBytes []interface{}
	valueBytes = value.([]interface{})
	if len(valueBytes) == 0 {
		return "", nil
	}
	element := string(valueBytes[1].([]byte))

	return element, nil
}
