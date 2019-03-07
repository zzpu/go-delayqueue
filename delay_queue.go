package delayqueue

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego"
	"ranbbService/models"
	"time"
	"github.com/astaxie/beego/orm"
	"ranbbService/util/cache"
)

var (
	// 每个定时器对应一个bucket
	timers []*time.Ticker
	// bucket名称chan
	bucketNameChan <-chan string

	session_redis *cache.RedisCache
)


// Init 初始化延时队列
func InitDelayQueue() error{
    var err error

	session_redis, err = cache.NewRedisCacheFromCfg("session_redis")
	if err!=nil{
		beego.Error("InitDelayQueue,err=",err)
		return err
	}
	//初始化配置，如果配置文件中没有，则使用默认配置
	Setting = &Config{}
	Setting.configParse()

	//redis连接池
	RedisPool = initRedisPool()

	//初始化各个任务桶的轮询定时器
	initTimers()
	bucketNameChan = generateBucketName()


	//初始化时添加Jobs
	err = InitJobsTable()
	if err!=nil{
		beego.Error("InitDelayQueue,err=",err)
		return err
	}


	//处理信誉过低恢复
	go handleScore()
	//处理任务定时
	go handleGoods()
	//处理订单定时
	go handleOrders()

	return nil
}
func InitJobsTable() error {

	//处理任务
	goods,count,err := models.DelayQueueGoods(0)
	if err !=nil{
		beego.Error("AddJobsTable,err=",err)
		return err
	}
	beego.Debug("Goods Jobs counts:",count)
	for _,goods:=range goods{
		job := Job{
			Topic:"GOODS",
			No:goods.No,
			Delay:goods.ExecTime,
			TTR:10,//十秒超时
			UUID:goods.UUID,
		}
		beego.Debug("Push goods:",job.No)
		err = Push(job)
		if err !=nil{
			beego.Error("AddJobsTable,err=",err)
			return err
		}
	}

	//处理订单
	orders,count,err := models.DelayQueueOrders(0)
	if err !=nil{
		beego.Error("AddJobsTable,err=",err)
		return err
	}
	beego.Debug("Orders Jobs counts:",count)
	for _,order:=range orders{
		job := Job{
			Topic:"ORDERS",
			No:order.No,
			Delay:order.ExecTime,
			TTR:10,//十秒超时
			UUID:order.UUID,
		}
		beego.Debug("Push orders:",job.No)
		err = Push(job)
		if err !=nil{
			beego.Error("AddJobsTable,err=",err)
			return err
		}
	}

	return nil
}

//处理信誉值过低用户处罚恢复
func handleScore(){
	for{
		job,err := Pop("SCORE")
		if err!=nil{
			beego.Error("handleScore,err=",err)
			continue
		}
		gScore,err := session_redis.HGetI("global","Score")
		if err !=nil{
			beego.Error("handleScore,err=",err)
			continue
		}
		if job !=nil{
			beego.Debug("handleScore,UUID=",job.UUID)
			user := &models.User{
				UUID:job.UUID,
				Score:int(gScore)+5,
				Punishment:0,
			}
			err = models.AdminResetScore(user)
			if err!=nil{
				beego.Error("handleScore ClearOvertimeGoods,err=",err)
				//如果不存在可能已经被下架了,直接清除任务
				if err == orm.ErrNoRows{
					err = Remove(job.No)
					if err!=nil{
						beego.Error("handleScore Remove,err=",err)
						continue
					}
				}
				continue
			}
			err = Remove(job.No)
			if err!=nil{
				beego.Error("handleScore Remove,err=",err)
				continue
			}
		}
	}
}

//处理任务超时协程
func handleGoods(){
	for{
		job,err := Pop("GOODS")
		if err!=nil{
			beego.Error("handleGoods,err=",err)
			continue
		}
        if job !=nil{
        	beego.Debug("handleGoods,UUID=",job.UUID," ,No=",job.No)
			_,err = models.PutOffGoods(job.UUID,job.No,nil)
			if err!=nil{
				beego.Error("handleGoods ClearOvertimeGoods,err=",err)
				//如果不存在可能已经被下架了,直接清除任务
				if err == models.ErrGoodsNotFount{
					err = Remove(job.No)
					if err!=nil{
						beego.Error("handleGoods Remove,err=",err)
						continue
					}
				}
				continue
			}
			err = Remove(job.No)
			if err!=nil{
				beego.Error("handleGoods Remove,err=",err)
				continue
			}
		}
	}
}

//处理订单超时协程
func handleOrders(){
	for{
		job,err := Pop("ORDERS")
		if err!=nil{
			beego.Error("handleGoods,err=",err)
			continue
		}

		if job !=nil{
			beego.Debug("handleOrders,UUID=",job.UUID," ,No=",job.No)
			err = models.ClearOvertimeOrder(job.UUID,job.No,SetUserInfoCache,PushOrders)
			if err!=nil{
				beego.Error("handleOrders ClearOvertimeOrder,err=",err)
				if err == orm.ErrNoRows{
					err = Remove(job.No)
					if err!=nil{
						beego.Error("handleOrders Remove,err=",err)
						continue
					}
				}
				continue
			}
			err = Remove(job.No)
			if err!=nil{
				beego.Error("handleOrders Remove,err=",err)
				continue
			}
		}
	}
}

// Push 添加一个Job到队列中
func Push(job Job) error {
	if job.No == "" || job.Topic == "" || job.Delay < 0 || job.TTR <= 0 {
		return errors.New("invalid job")
	}
	//将任务添加到任务池（redis集合）
	err := putJob(job.No, job)
	if err != nil {
		beego.Error("添加job到job pool失败#job-%+v#%s", job, err.Error())
		return err
	}

	//将任务添加到任务桶（redis有序集合）
	err = pushToBucket(<-bucketNameChan, job.Delay, job.No)
	if err != nil {
		beego.Error("添加job到bucket失败#job-%+v#%s", job, err.Error())
		return err
	}

	return nil
}

// Pop 轮询获取Job
func Pop(topic string) (*Job, error) {
	queueName := fmt.Sprintf(Setting.QueueName, topic)
	jobNo, err := blockPopFromReadyQueue(queueName, Setting.QueueBlockTimeout)
	if err != nil {

		return nil, err
	}

	// 队列为空
	if jobNo == "" {
		return nil, nil
	}

	// 获取job元信息
	job, err := getJob(jobNo)
	if err != nil {
		return job, err
	}

	// 消息不存在, 可能已被删除---则不会放到放回到延时队列
	if job == nil {
		beego.Debug("job was removed")
		return nil, nil
	}

	//放回到延时队列---->执行完任务必须删除任务，否则这里会导致任务重复投递
	timestamp := time.Now().Unix() + job.TTR
	err = pushToBucket(<-bucketNameChan, timestamp, job.No)

	return job, err
}

// Remove 删除Job
func Remove(jobNo string) error {
	return removeJob(jobNo)
}

// Get 查询Job
func Get(jobNo string) (*Job, error) {
	job, err := getJob(jobNo)
	if err != nil {
		return job, err
	}

	// 消息不存在, 可能已被删除
	if job == nil {
		return nil, nil
	}
	return job, err
}

// 轮询获取bucket名称, 使job分布到不同bucket中, 提高扫描速度
func generateBucketName() <-chan string {
	c := make(chan string)
	go func() {
		i := 1
		for {
			//轮询不通bucket
			c <- fmt.Sprintf(Setting.BucketName, i)
			if i >= Setting.BucketSize {
				i = 1
			} else {
				i++
			}
		}
	}()

	return c
}

// 初始化定时器
func initTimers() {
	timers = make([]*time.Ticker, Setting.BucketSize)
	var bucketName string
	for i := 0; i < Setting.BucketSize; i++ {
		timers[i] = time.NewTicker(1 * time.Second)
		bucketName = fmt.Sprintf(Setting.BucketName, i+1)
		beego.Debug("Init delay queue bucket:",bucketName)
		go waitTicker(timers[i], bucketName)
	}
}

func waitTicker(timer *time.Ticker, bucketName string) {
	for {
		select {
		case t := <-timer.C:
			tickHandler(t, bucketName)
		}
	}
}

// 扫描bucket, 取出延迟时间小于当前时间的Job
func tickHandler(t time.Time, bucketName string) {
	for {
		bucketItem, err := getFromBucket(bucketName)
		if err != nil {
			beego.Error("扫描bucket错误#bucket-%s#%s", bucketName, err.Error())
			return
		}

		// 集合为空
		if bucketItem == nil {
			//beego.Debug("Got bucketItem is nil")
			return
		}

		// 延迟时间未到
		if bucketItem.timestamp > t.Unix() {
			if (bucketItem.timestamp - t.Unix()) < 10{
				beego.Debug("Not the time for executing job:",bucketItem.jobNo,"need time:",bucketItem.timestamp - t.Unix(),"s")
			}
			return
		}

		// 延迟时间小于等于当前时间, 取出Job元信息并放入ready queue
		job, err := getJob(bucketItem.jobNo)
		if err != nil {
			beego.Error("获取Job元信息失败#bucket-%s#%s", bucketName, err.Error())
			continue
		}

		// job元信息不存在, 从bucket中删除
		if job == nil {
			removeFromBucket(bucketName, bucketItem.jobNo)
			continue
		}

		// 再次确认元信息中delay是否小于等于当前时间
		if job.Delay > t.Unix() {
			// 从bucket中删除旧的jobNo
			removeFromBucket(bucketName, bucketItem.jobNo)


			// 重新计算delay时间并放入bucket中
			pushToBucket(<-bucketNameChan, job.Delay, bucketItem.jobNo)
			continue
		}


		//beego.Debug("pushToReadyQueue:",job.Topic,",No:",bucketItem.jobNo)
		err = pushToReadyQueue(job.Topic, bucketItem.jobNo)
		if err != nil {
			beego.Error("JobNo放入ready queue失败#bucket-%s#job-%+v#%s",
				bucketName, job, err.Error())
			continue
		}

		// 从bucket中删除
		removeFromBucket(bucketName, bucketItem.jobNo)
	}
}
func SetUserInfoCache(i interface{})error{
	user := i.(*models.User)

	err := session_redis.HSet(user.UUID,"Score",user.Score,0)
	if err !=nil{
		return err
	}

	//设置正在进行的任务数
	err= session_redis.HSet(user.UUID,"Counter",user.Counter,0)
	if err !=nil{
		return err
	}

	gScore,err := session_redis.HGetI("global","Score")
	if err !=nil{
		return err
	}
	//如果信誉值低于最低信誉值,写入解锁时间
	if int64(user.Score)<gScore{
		gTime,err := session_redis.HGetI("global","BanTime")
		if err !=nil{
			return err
		}
		err = session_redis.HSet(user.UUID,"Unlock",time.Now().Add(time.Duration(gTime)*time.Hour).Unix(),0)
		if err !=nil{
			return err
		}
	}

	return nil
}

func PushOrders(i interface{})error{
	order := i.(*models.Orders)
	job := Job{
		Topic:"ORDERS",
		No:order.No,
		Delay:order.ExecTime,
		TTR:10,//十秒超时
		UUID:order.UUID,
	}
	beego.Debug("Push order job:",job.No)
	err := Push(job)
	if err !=nil{
		beego.Error("AddJobsTable,err=",err)
		return err
	}

	return nil
}