package delayqueue

import (

	"github.com/astaxie/beego"
)

// 解析配置文件

var (
	// Setting 配置实例
	Setting *Config
)

const (
	// DefaultBindAddress 监听地址
	DefaultBindAddress         = "0.0.0.0:9277"
	// DefaultBucketSize bucket数量
	DefaultBucketSize          = 3
	// DefaultBucketName bucket名称
	DefaultBucketName          = "dq_bucket_%d"
	// DefaultQueueName 队列名称
	DefaultQueueName           = "dq_queue_%s"
	// DefaultQueueBlockTimeout 轮询队列超时时间
	DefaultQueueBlockTimeout   = 20
	// DefaultRedisHost Redis连接地址
	DefaultRedisHost           = "127.0.0.1:6379"
	// DefaultRedisDb Redis数据库编号
	DefaultRedisDb             = 2
	// DefaultRedisPassword Redis密码
	DefaultRedisPassword       = ""
	// DefaultRedisMaxIdle Redis连接池闲置连接数
	DefaultRedisMaxIdle        = 10
	// DefaultRedisMaxActive Redis连接池最大激活连接数, 0为不限制
	DefaultRedisMaxActive      = 0
	// DefaultRedisConnectTimeout Redis连接超时时间,单位毫秒
	DefaultRedisConnectTimeout = 5000
	// DefaultRedisReadTimeout Redis读取超时时间, 单位毫秒
	DefaultRedisReadTimeout    = 180000
	// DefaultRedisWriteTimeout Redis写入超时时间, 单位毫秒
	DefaultRedisWriteTimeout   = 3000
)

// Config 应用配置
type Config struct {
	BindAddress       string      // http server 监听地址
	BucketSize        int         // bucket数量
	BucketName        string      // bucket在redis中的键名,
	QueueName         string      // ready queue在redis中的键名
	QueueBlockTimeout int         // 调用blpop阻塞超时时间, 单位秒, 修改此项, redis.read_timeout必须做相应调整
	Redis             RedisConfig // redis配置
}

// RedisConfig Redis配置
type RedisConfig struct {
	Host           string
	Db             int
	Password       string
	MaxIdle        int // 连接池最大空闲连接数
	MaxActive      int // 连接池最大激活连接数
	ConnectTimeout int // 连接超时, 单位毫秒
	ReadTimeout    int // 读取超时, 单位毫秒
	WriteTimeout   int // 写入超时, 单位毫秒
}



// 解析配置文件
func (config *Config) configParse() {

	config.BindAddress = beego.AppConfig.DefaultString("bind_address",DefaultBindAddress)
	config.BucketSize = beego.AppConfig.DefaultInt("bucket_size",DefaultBucketSize)
	config.BucketName = beego.AppConfig.DefaultString("bucket_name",DefaultBucketName)
	config.QueueName = beego.AppConfig.DefaultString("queue_name",DefaultQueueName)
	config.QueueBlockTimeout = beego.AppConfig.DefaultInt("queue_block_timeout",DefaultQueueBlockTimeout)

	config.Redis.Host = beego.AppConfig.DefaultString("redis.host",DefaultRedisHost)
	config.Redis.Db = beego.AppConfig.DefaultInt("redis.db",DefaultRedisDb)
	config.Redis.Password = beego.AppConfig.DefaultString("redis.password",DefaultRedisPassword)
	config.Redis.MaxIdle = beego.AppConfig.DefaultInt("redis.max_idle",DefaultRedisMaxIdle)
	config.Redis.MaxActive = beego.AppConfig.DefaultInt("redis.max_active",DefaultRedisMaxActive)
	config.Redis.ConnectTimeout = beego.AppConfig.DefaultInt("redis.connect_timeout",DefaultRedisConnectTimeout)
	config.Redis.ReadTimeout = beego.AppConfig.DefaultInt("redis.read_timeout",DefaultRedisReadTimeout)
	config.Redis.WriteTimeout = beego.AppConfig.DefaultInt("redis.write_timeout",DefaultRedisWriteTimeout)
}
