package gormkeyvalue

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

const (
	dbDriver                  = "mysql"
	dbLoggerSlowSQLTreshold   = time.Second * 3
	dbLoggerLevel             = logger.Warn
	dbLoggerColorEnabled      = true
	dbLoggerIgnoreNotFoundErr = true
)

var models = []interface{}{&Entry{}}

type dbHandler struct {
	conn *sql.DB
	gorm *gorm.DB

	tablesPrefix string
}

type DBConfig struct {
	Host          string `json:"DB_HOST" envconfig:"DB_HOST" default:"localhost"`
	Port          int    `json:"DB_PORT" envconfig:"DB_PORT" default:"3306"`
	Name          string `json:"DB_NAME" envconfig:"DB_NAME" required:"true"`
	User          string `json:"DB_USER" envconfig:"DB_USER" required:"true"`
	Password      string `json:"DB_PASSWORD" envconfig:"DB_PASSWORD" default:""`
	ConnTimeoutMS int    `json:"DB_CONN_TIMEOUT" envconfig:"DB_CONN_TIMEOUT" default:"5000"`
	TablePrefix   string `json:"DB_TABLE_PREFIX" envconfig:"DB_TABLE_PREFIX" default:""`

	MaxOpenConns        int `json:"DB_MAX_OPEN_CONNS" envconfig:"DB_MAX_OPEN_CONNS" default:"10"`
	MaxIdleConns        int `json:"DB_MAX_IDLE_CONNS" envconfig:"DB_MAX_IDLE_CONNS" default:"5"`
	ConnMaxLifetimeMins int `json:"DB_CONN_MAX_LIFETIME_MINS" envconfig:"DB_CONN_MAX_LIFETIME_MINS" default:"5"`

	GormDebugMode bool   `json:"DB_GORM_DEBUG_MODE" envconfig:"DB_GORM_DEBUG_MODE" default:"false"`
	Location      string `json:"DB_TIME_LOCATION" envconfig:"DB_TIME_LOCATION" default:"Europe/Moscow"`
}

type Memory interface {
	IsEntryExists(Entry) (bool, error)
	GetAllEntrys() ([]Entry, error)
	GetEntrysLikeName(namePattern string) ([]Entry, error)
	GetEntry(key string) (Entry, error)
	SaveEntry(e Entry) error
}

type Entry struct {
	ID        uint64    `gorm:"primarykey"`
	CreatedAt time.Time `gorm:"index"`
	UpdatedAt time.Time `gorm:"index"`

	Key   string `gorm:"index"`
	Name  string `gorm:"index"`
	Value []byte `gorm:"type:json"`
}

func GetDBConnectionURI(cfg DBConfig) string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?timeout=%dms&parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, cfg.ConnTimeoutMS,
	)
}

func New(cfg DBConfig) (Memory, error) {
	lg := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             dbLoggerSlowSQLTreshold,
			LogLevel:                  dbLoggerLevel,
			IgnoreRecordNotFoundError: dbLoggerIgnoreNotFoundErr,
			Colorful:                  dbLoggerColorEnabled,
		},
	)

	var err error
	var conn *sql.DB
	var connErr error
	if conn, err = sql.Open(dbDriver, GetDBConnectionURI(cfg)); err != nil {
		return nil, fmt.Errorf("open sqldb connection: %v", err)
	}
	if connErr != nil {
		return nil, fmt.Errorf("db conn error: %w", err)
	}

	conn.SetMaxOpenConns(cfg.MaxOpenConns)

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	mysqlConnConfig := mysql.New(mysql.Config{
		Conn: conn,
	})

	prefix := ""
	if cfg.TablePrefix != "" {
		prefix = fmt.Sprintf("%s_", cfg.TablePrefix)
	}

	gormConfig := &gorm.Config{
		SkipDefaultTransaction:   true,
		DisableNestedTransaction: true,
		Logger:                   lg,
		NowFunc: func() time.Time {
			ti, err := time.LoadLocation(cfg.Location)
			if err != nil {
				panic(err)
			}

			return time.Now().In(ti)
		},
		NamingStrategy: schema.NamingStrategy{
			TablePrefix: prefix,
		},
	}

	gormConn, err := gorm.Open(mysqlConnConfig, gormConfig)
	if err != nil {
		return nil, fmt.Errorf("open gorm conn: %w", err)
	}

	// migrate
	for _, prefab := range models {
		if err := gormConn.AutoMigrate(prefab); err != nil {
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}

	return &dbHandler{
		conn:         conn,
		gorm:         gormConn,
		tablesPrefix: prefix,
	}, nil
}

func (db *dbHandler) IsEntryExists(e Entry) (bool, error) {
	result := db.gorm.Where(&e).First(&e)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, result.Error
	}
	return true, nil
}

func (db *dbHandler) GetAllEntrys() ([]Entry, error) {
	entrys := []Entry{}

	result := db.gorm.Model(&Entry{}).Find(&entrys)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	return entrys, result.Error
}

func (db *dbHandler) GetEntrysLikeName(namePattern string) ([]Entry, error) {
	entrys := []Entry{}

	result := db.gorm.Model(&Entry{}).Where("key = ?", namePattern).Find(&entrys)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	return entrys, result.Error
}

func (db *dbHandler) GetEntry(key string) (Entry, error) {
	e := Entry{Key: key}
	err := db.gorm.Model(&Entry{}).Where(&e).First(&e).Error
	return e, err
}

func (db *dbHandler) SaveEntry(e Entry) error {
	if err := db.gorm.Save(&e).Error; err != nil {
		return fmt.Errorf("save entry: %w", err)
	}
	return nil
}
