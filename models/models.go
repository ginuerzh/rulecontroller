package models

import (
	"encoding/json"
	"github.com/shevilangle/rulecontroller/errors"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	databaseName = "sports"
	accountColl  = "accounts"
	ruleColl     = "rules"
	tableColl    = "stable"
	MongoAddr    = "localhost:27017"
	mgoSession   *mgo.Session
)

type MsgBody struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

type EventData struct {
	Type string    `json:"type"`
	Id   string    `json:"pid"`
	From string    `json:"from"`
	To   string    `json:"to"`
	Body []MsgBody `json:"body"`
}

type Event struct {
	Id   bson.ObjectId `bson:"_id,omitempty" json:"-"`
	Type string        `json:"type"`
	Data EventData     `json:"push"`
	Time int64         `json:"time"`
}

type Location struct {
	Lat float64 `bson:"latitude" json:"latitude"`
	Lng float64 `bson:"longitude" json:"longitude"`
}

type Account struct {
	Id  string    `bson:"_id,omitempty" json:"-"`
	Loc *Location `bson:",omitempty" json:"-"`
}

type SearchTable struct {
	Id         int    `bson:"_id" json:"-"`
	Condiction string `bson:"condiction" json:"-"`
	Content    string `bson:"content" json:"-"`
}

func (this *Account) findOne(query interface{}) (bool, error) {
	var users []Account

	err := search(accountColl, query, nil, 0, 1, nil, nil, &users)
	if err != nil {
		return false, errors.NewError(errors.DbError, err.Error())
	}
	if len(users) > 0 {
		*this = users[0]
	}
	return len(users) > 0, nil
}

func (this *Account) FindByUserid(userid string) (bool, error) {
	return this.findOne(bson.M{"_id": userid})
}

// parse rule string
//fileds keywords:reg_time, gender, loc, height, weight, lastlogin, birth, devices, hobby
//relations keywords:within, littlethan, greaterthan, equal, unequal, between
//rulestr := "loc within 1 and years littlethan 30 and gender equal male and weight between 65,80 and height greaterthan 170"

func parseStrToQuery(ruleStr, userid string) (bson.M, int) {
	query := bson.M{}
	var limit int
	fields := strings.Split(ruleStr, " and ")
	for _, field := range fields {
		fs := strings.Split(field, " ")
		switch fs[0] {
		case "loc":
			fallthrough
		case "height":
			fallthrough
		case "weight":
			fallthrough
		case "birth":
			fallthrough
		case "regtime":
			fallthrough
		case "lastlogin":
			log.Println("fs[1] :", fs[1])
			log.Println("fs[2] :", fs[2])
			v1, _ := strconv.Atoi(fs[2])
			if fs[1] == "equal" {
				query[fs[0]] = v1
			} else if fs[1] == "littlethan" {
				query[fs[0]] = bson.M{
					"$lt": v1,
				}
			} else if fs[1] == "greaterthan" {
				query[fs[0]] = bson.M{
					"$gt": v1,
				}
			} else if fs[1] == "between" {
				vs := strings.Split(fs[2], ",")
				vv1, _ := strconv.Atoi(vs[0])
				vv2, _ := strconv.Atoi(vs[1])
				query[fs[0]] = bson.M{
					"$lt": vv2,
					"$gt": vv1,
				}
			} else if fs[1] == "unequal" {
				query[fs[0]] = bson.M{
					"$ne": v1,
				}
			} else if fs[1] == "within" {
				//means loc
				c := float64(v1*1000) / float64(111319)
				log.Println("userid: ", userid)

				user := &Account{}
				if find, _ := user.FindByUserid(userid); find {
					if user.Loc == nil {
						testdata := &Location{
							Lat: 31.201122,
							Lng: 121.601551,
						}
						query["loc"] = bson.M{
							"$near":        []float64{testdata.Lat, testdata.Lng},
							"$maxDistance": c,
						}
						query["_id"] = bson.M{
							"$ne": userid,
						}
						//return query, 0
					} else {
						query["loc"] = bson.M{
							"$near":        []float64{user.Loc.Lat, user.Loc.Lng},
							"$maxDistance": c,
						}
						query["_id"] = bson.M{
							"$ne": userid,
						}
					}
				}
			}

		case "gender":
			fallthrough
		case "devices":
			fallthrough
		case "hobby":
			if fs[1] == "equal" {
				query[fs[0]] = fs[2]
			} else if fs[1] == "littlethan" {
				query[fs[0]] = bson.M{
					"$lt": fs[2],
				}
			} else if fs[1] == "greaterthan" {
				query[fs[0]] = bson.M{
					"$gt": fs[2],
				}
			} else if fs[1] == "between" {
				vs := strings.Split(fs[2], ",")
				query[fs[0]] = bson.M{
					"$lt": vs[1],
					"$gt": vs[0],
				}
			} else if fs[1] == "unequal" {
				query[fs[0]] = bson.M{
					"$ne": fs[2],
				}
			} else if fs[1] == "within" {
				/*
					//means loc
					c := float64(fs[2]*1000) / float64(111319)
					userid := events.Data.From
					log.Println("userid: ", userid)

					user := &Account{}
					if find, _ := user.FindByUserid(userid); find {
						query["loc"] = bson.M{
							"$near":        []float64{user.Loc.Lat, user.Loc.Lng},
							"$maxDistance": c,
						}
						query["_id"] = bson.M{
							"$ne": userid,
						}
					}
				*/
			}
		case "age":
			vs := strings.Split(fs[2], ",")
			age1, _ := strconv.Atoi(vs[0])
			realTime := time.Now().Unix() - int64((age1*365+age1/4)*24*3600)
			if realTime > 0 {
				realTime = time.Now().Unix()
			}
			if fs[1] == "equal" {
				query["birth"] = realTime
			} else if fs[1] == "littlethan" {
				query["birth"] = bson.M{
					"$lt": realTime,
				}
			} else if fs[1] == "greaterthan" {
				query["birth"] = bson.M{
					"$gt": realTime,
				}
			} else if fs[1] == "between" {
				age2, _ := strconv.Atoi(vs[1])
				realTime2 := time.Now().Unix() - int64((age2*365+age2/4)*24*3600)
				if realTime2 > 0 {
					realTime2 = time.Now().Unix()
				}
				query["birth"] = bson.M{
					"$lt": realTime2,
					"$gt": realTime,
				}
			}
		case "count":
			if fs[1] == "equal" {
				limit, _ = strconv.Atoi(fs[2])
			} else if fs[1] == "littlethan" {
				limit, _ = strconv.Atoi(fs[2])
			}
		default:
			log.Println("fs is: ", fs)
		}
	}

	log.Println("query is: ", query, ", and limit is ", limit)
	return query, limit - 1
}

func RebuildPushData(data []byte) (error, []byte) {
	var events Event
	var users []Account
	var limit int

	err := json.Unmarshal(data, &events)
	if err != nil {
		log.Println(err)
		return err, data
	}

	rule_id := 0
	ruleStr := ""
	for _, body := range events.Data.Body {
		if body.Type == "rule" {
			rule_id, _ = strconv.Atoi(body.Content)
			break
		}
	}
	//log.Println("rule_id :", rule_id)
	if rule_id > 0 {
		var items []SearchTable
		q := bson.M{
			"_id": rule_id,
		}
		err = search(tableColl, q, nil, 0, 1, nil, nil, &items)
		if err != nil {
			return err, data
		}
		if len(items) > 0 {
			ruleStr = items[0].Condiction
		}
	} else {
		return errors.NewError(errors.DbError, err.Error()), data
	}
	//log.Println("ruleStr :", ruleStr)
	if len(ruleStr) == 0 {
		log.Println("no rule filed, err")
		return errors.NewError(errors.DbError, err.Error()), data
	}

	query, limit := parseStrToQuery(ruleStr, events.Data.From)
	total := 0
	if err = search(accountColl, query, nil, 0, limit, nil, &total, &users); err != nil {
		return errors.NewError(errors.DbError, err.Error()), data
	}

	receivers := ""
	if len(users) > 0 {
		for i, u := range users {
			if i > 0 {
				receivers = receivers + "  "
			}
			log.Println("u.Id is :", u.Id)
			receivers = receivers + u.Id
		}
	}

	newbody := make([]MsgBody, len(events.Data.Body)+1)
	j := 0
	for _, m := range events.Data.Body {
		newbody[j] = m
		j++
	}
	newbody[j].Type = "receiver"
	newbody[j].Content = receivers
	log.Println("receivers is :", receivers)
	result := &Event{
		Type: events.Type,
		Time: events.Time,
		Data: EventData{
			Type: events.Data.Type,
			Id:   events.Data.Id,
			From: events.Data.From,
			Body: newbody,
		},
	}
	rd, errs := json.Marshal(result)
	return errs, rd
}

func search(collection string, query interface{}, selector interface{},
	skip, limit int, sortFields []string, total *int, result interface{}) error {

	q := func(c *mgo.Collection) error {
		qy := c.Find(query)
		var err error

		if selector != nil {
			qy = qy.Select(selector)
		}

		if total != nil {
			if *total, err = qy.Count(); err != nil {
				return err
			}
		}

		if result == nil {
			return err
		}

		if limit > 0 {
			qy = qy.Limit(limit)
		}
		if skip > 0 {
			qy = qy.Skip(skip)
		}
		if len(sortFields) > 0 {
			qy = qy.Sort(sortFields...)
		}

		return qy.All(result)
	}

	if err := withCollection(collection, nil, q); err != nil {
		return errors.NewError(errors.DbError, err.Error())
	}
	return nil
}

func getSession() *mgo.Session {
	if mgoSession == nil {
		var err error
		mgoSession, err = mgo.Dial(MongoAddr)
		//log.Println(MongoAddr)
		if err != nil {
			log.Println(err) // no, not really
		}
	}
	return mgoSession.Clone()
}

func withCollection(collection string, safe *mgo.Safe, s func(*mgo.Collection) error) error {
	session := getSession()
	defer session.Close()

	session.SetSafe(safe)
	c := session.DB(databaseName).C(collection)
	return s(c)
}

func SaveToDB(collection string, o interface{}, safe bool) error {
	var err error
	insert := func(c *mgo.Collection) error {
		return c.Insert(o)
	}

	if safe {
		err = withCollection(collection, &mgo.Safe{}, insert)
	} else {
		err = withCollection(collection, nil, insert)
	}

	if err != nil {
		log.Println(err)
		return errors.NewError(errors.DbError, err.(*mgo.LastError).Error())
	}

	return nil
}
