/*
@Time : 2019/7/12 10:50 
@Author : Tester
@File : 一条小咸鱼
@Software: GoLand
*/
package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

//
//Addrs= "192.168.5.234"
//Database =   "game_report"
//Source =   "admin"
//Username =  "test1"
//Password =  "123456"

// mongo 配置文件
type MongoConfig struct {
	Addrs     string
	Database    string
	Username string
	Password string
	FilterData string

}


func main() {
	p, err := ReadConf("./MongoConfig.toml")
	if err != nil{
		log.Fatal("获取配置表错误-->",err)
		return
	}

	info := configToDialInfo(p)
	log.Println("Mongo配置-->",info)
	session, err := mgo.DialWithInfo(info)
	defer session.Close()
	names, err := session.DatabaseNames()
	if err != nil{
		log.Fatal("获取数据库错误-->",err)
		return
	}
	var dataBaseName string
	for k,name :=  range names{
		fmt.Println("输入:",k,"选择数据库->",name)
	}
	fmt.Scan(&dataBaseName)
	i, err := strconv.Atoi(dataBaseName)
	if err != nil{
		log.Fatal("转换数字错误-->",err)
		return
	}
	db := session.DB(names[i])
	collections, err := db.CollectionNames()
	if err != nil{
		log.Fatal("获取表数据错误-->",err)
		return
	}
	var filterName string
	var f string
	//for _,name :=  range collections{
	//	fmt.Println(name)
	//}

	log.Println("请输入不删除数据的表多(多张表通过 , 隔开):")


	split := strings.Split(p.FilterData, ",")

	fmt.Println("数据库:",names[i])
	fmt.Println("不需要删除的表:",p.FilterData)
	fmt.Println("输入yes同意执行,no退出程序:")
	fmt.Scan(&f)
	if f != "yes"{
		return
	}
	A:for _,name :=  range collections{

		//判断是否是需要过滤的数据
		for _,filterName := range split {
		 if name == filterName{
		 	 //不需要过滤的数据
			 log.Println("不需要删除的数据表:",name,",跳过")
			 continue A
		 }

		}
		_, err := db.C(name).RemoveAll(nil)
		log.Println("删除数据表:",name)
		if err != nil{
			log.Println("删除表-->",name,",错误-->",err)
		}
		log.Println("删除数据表:",name)
	}
	fmt.Println("按任意键退出!")
	fmt.Scan(&filterName)
}




func configToDialInfo(m *MongoConfig )*mgo.DialInfo{
	dail_info := &mgo.DialInfo{
		Addrs:     []string{m.Addrs},
		Timeout:   time.Second * 5,
		Database:  m.Database,
		Username:  m.Username,
		Password:  m.Password,
	}
	return dail_info
}



func ReadConf(fname string) (p *MongoConfig, err error) {
	var (
		fp       *os.File
		fcontent []byte
	)
	p = new(MongoConfig)
	if fp, err = os.Open(fname); err != nil {
		fmt.Println("open error ", err)
		return
	}

	if fcontent, err = ioutil.ReadAll(fp); err != nil {
		fmt.Println("ReadAll error ", err)
		return
	}

	if err = toml.Unmarshal(fcontent, p); err != nil {
		fmt.Println("toml.Unmarshal error ", err)
		return
	}
	return
}

