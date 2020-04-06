package main

import (
    "database/sql"
    "fmt"
    "github.com/alexeyco/simpletable"
    "github.com/fatih/color"
    _ "github.com/go-sql-driver/mysql"
    "log"
    "strings"
    "time"
)

// event_time           user_host	                thread_id	server_id	command_type    argument
// 2020-04-06 22:13:34  root[root] @ localhost []	196         1	        Query           SELECT DATABASE ()
type MySQLObject struct {
    EventTime   string
    UserHost    string
    ThreadId    int
    ServerId    int
    CommandType string
    Argument    string
}

type LimitNum struct {
    Num int
}

//const (
//    USERNAME = "root"
//    PASSWORD = "root"
//    NETWORK  = "tcp"
//    SERVER   = "127.0.0.1"
//    PORT     = 3306
//    DATABASE = "mysql"
//)

//root:pass@tcp(ip:port)/mysql
func getArgs() string {
    var conn string
    fmt.Println("Please enter as follows:   root:pass@tcp(ip:port)/mysql")
    fmt.Printf("conn: ")
    _, err := fmt.Scan(&conn)
    if err != nil {
        log.Fatal("Input conn err:", err)
    }
    return conn
}

func SetGlobal(DB *sql.DB) {
    if _, err := DB.Exec("SET GLOBAL general_log=on"); err != nil {
        log.Fatal("SET GLOBAL general Err:", err)
        return
    }
    if _, err := DB.Exec("SET GLOBAL log_output='table'"); err != nil {
        log.Fatal("SET GLOBAL log_output Err:", err)
        return
    }
}

func QueryCount(DB *sql.DB) int {
    limitNum := new(LimitNum)
    row := DB.QueryRow("SELECT count(event_time) FROM general_log")
    if err := row.Scan(&limitNum.Num); err != nil {
        log.Fatal("Count Err:", err)
        return 0
    }
    return limitNum.Num
}

func QueryMulti(DB *sql.DB, LimitNum int) {
    MySQLObject := new(MySQLObject)
    rows, err := DB.Query("SELECT * FROM general_log LIMIT ?,4294967296", LimitNum)
    defer func() {
        if rows != nil {
            _ = rows.Close()
        }
    }()
    if err != nil {
        log.Fatal("Query Err:", err)
        return
    }

    table := simpletable.New()
    //table.Header = &simpletable.Header{
    //    Cells: []*simpletable.Cell{
    //        {Align: simpletable.AlignCenter, Text: "event_time"},
    //        //{Align: simpletable.AlignCenter, Text: "user_host"},
    //        //{Align: simpletable.AlignCenter, Text: "thread_id"},
    //        //{Align: simpletable.AlignCenter, Text: "server_id"},
    //        {Align: simpletable.AlignCenter, Text: "command_type"},
    //        {Align: simpletable.AlignCenter, Text: "argument"},
    //    },
    //}

    for rows.Next() {
        err = rows.Scan(&MySQLObject.EventTime, &MySQLObject.UserHost, &MySQLObject.ThreadId, &MySQLObject.ServerId, &MySQLObject.CommandType, &MySQLObject.Argument)
        if err != nil {
            log.Fatal("Scan Err:", err)
            return
        }

        r := []*simpletable.Cell{
            {Text: MySQLObject.EventTime},
            {Text: MySQLObject.CommandType},
            {Text: MySQLObject.Argument},
        }
        if strings.Contains(MySQLObject.Argument, "general_log") || strings.Contains(MySQLObject.Argument, "stmt") {
            break
        }
        table.Body.Cells = append(table.Body.Cells, r)
        //fmt.Println("| ", MySQLObject.EventTime, " | ", MySQLObject.UserHost, " | ", MySQLObject.ThreadId, " | ", MySQLObject.ServerId, " | ", MySQLObject.CommandType, " | ", MySQLObject.Argument, " |")
    }

    table.SetStyle(simpletable.StyleCompact)
    if len(table.Body.Cells) != 0 {
        color.Cyan("event_time\tcommand_type\targument")
        color.Cyan("------------------------------------------------------------------")
        color.Cyan(table.String())
        fmt.Printf("\n\n")
    }
}

func main() {
    //conn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", USERNAME, PASSWORD, NETWORK, SERVER, PORT, DATABASE)
    //fmt.Println(conn)
    conn := getArgs()
    DB, err := sql.Open("mysql", conn)
    if err != nil {
        log.Fatal("Connection Error:", err)
        return
    }

    SetGlobal(DB)

    LimitNum := 0
    LimitNumTmp := QueryCount(DB)
    for {
        if LimitNum != LimitNumTmp {
            LimitNum = LimitNumTmp
            QueryMulti(DB, LimitNum)
        }
        LimitNumTmp = QueryCount(DB)
        time.Sleep(1 * time.Second)
    }
}
