package mysql

import (
	"errors"
	"fmt"
	"log"
	"os"

	"timerTask/common/conf"

	mysql2 "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const DuplicateEntryErrCode = 1062

// Client 客户端
type Client struct {
	*gorm.DB
}

// GetClient 获取一个数据库客户端
func GetClient(confProvider *conf.MysqlConfProvider) (*Client, error) {
	conf := confProvider.Get()
	// 创建一个新的日志记录器实例
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // 使用标准库的 log 包输出日志
		logger.Config{
			LogLevel: logger.Info, // 设置日志级别
		},
	)
	db, err := gorm.Open(mysql.Open(conf.DSN), &gorm.Config{
		Logger: newLogger,
	})
	if err != nil {
		panic(fmt.Errorf("failed to connect database, err: %w", err))
	}
	_db, err := db.DB()
	if err != nil {
		panic(err)
	}
	_db.SetMaxOpenConns(conf.MaxOpenConns) //设置数据库连接池最大连接数
	_db.SetMaxIdleConns(conf.MaxIdleConns) //连接池最大允许的空闲连接数，如果没有sql任务需要执行的连接数大于20，超过的连接会被连接池关闭
	return &Client{DB: db}, nil
}

func NewClient(db *gorm.DB) *Client {
	return &Client{
		DB: db,
	}
}

func IsDuplicateEntryErr(err error) bool {
	var mysqlErr *mysql2.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == DuplicateEntryErrCode
}
