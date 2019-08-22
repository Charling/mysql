package mysql

import (
	"database/sql"
	"fmt"
	"log"
	_ "github.com/go-sql-driver/mysql"
	LOGGER "yn.com/ext/common/logger"
)

type Conn struct {
	DriverName string
	DataSourceName string
	MaxOpenConns int 		//用于设置最大打开的连接数，默认值为0表示不限制		
	MaxIdleConns int		//用于设置闲置的连接数
	SQLDB *sql.DB
}

func connectMySQL(host, database, user, password, charset string, maxOpenConns, maxIdleConns int) *Conn {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&autocommit=true", user, password, host, database, charset)
	db := &Conn {
		DriverName: "mysql",
		DataSourceName: dataSourceName,
		MaxOpenConns: maxOpenConns,
		MaxIdleConns: maxIdleConns,
	}
	var err error
	db.SQLDB, err = sql.Open(db.DriverName, db.DataSourceName)
	if err != nil {
		LOGGER.Error("open mysql failed err=", err)
		return nil
	}
	if err = db.SQLDB.Ping(); err != nil {
		LOGGER.Error("ping mysql failed err=", err)
		return nil
	}
	db.SQLDB.SetMaxOpenConns(db.MaxOpenConns)
	db.SQLDB.SetMaxIdleConns(db.MaxIdleConns)
	return db
}

func (db *Conn) close() error {
	return db.SQLDB.Close()
}

func (db *Conn) ping() error {
	return db.SQLDB.Ping()
}

func (db *Conn) execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return db.SQLDB.Exec(sqlStr, args...)
}

func (db *Conn) GetDB() *sql.DB {
	return db.SQLDB
}

func (db *Conn) Update(sqlStr string, args ...interface{}) (int64, error) {
	res, err := db.execute(sqlStr, args...)
	if err != nil {
		return 0, err
	}
	
	affect, err := res.RowsAffected()
	return affect, err
}

func (db *Conn) Insert(sqlStr string, args ...interface{}) (int64, error) {
	result, err := db.execute(sqlStr, args...)
	if err != nil {
		return 0, err
	}

	lastid, err := result.LastInsertId()
	return lastid, err
}

func (db *Conn) Delete(sqlStr string, args ...interface{}) (int64, error) {
	result, err := db.execute(sqlStr, args...)
	if err != nil {
		return 0, err
	}

	affect, err := result.RowsAffected()
	return affect, err
}

func (db *Conn) Query(sqlStr string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.SQLDB.Query(sqlStr, args...)
	if err != nil {
		log.Println(err)
		return []map[string]interface{}{}, err
	}

	defer rows.Close()
    columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
    rowsMap := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		rowMap := make(map[string]interface{})
		for i, col := range values {
			if col != nil {
				rowMap[columns[i]] = string(col.([]byte))
			}
		}
		rowsMap = append(rowsMap, rowMap)
	}
	
    if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}

	return rowsMap, nil
}
