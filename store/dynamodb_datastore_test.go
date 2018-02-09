package store

import (
	"strconv"
	"testing"

	"gitlab.com/paasapi/api/common/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

type TestDbError struct {
	EMessage  string
	ECode     string
	OrigError error
}

func (t TestDbError) Code() string {
	return t.ECode
}
func (t TestDbError) Message() string {
	return t.EMessage
}
func (t TestDbError) OrigErr() error {
	return t.OrigError
}
func (t TestDbError) Error() string {
	return t.EMessage
}

func getDbClient() *dynamodb.DynamoDB {
	dbc := dynamodb.New(session.New(), aws.NewConfig().WithRegion("us-west-2"))

	dbc.Handlers.Send.Clear()
	dbc.Handlers.ValidateResponse.Clear()
	dbc.Handlers.UnmarshalMeta.Clear()
	dbc.Handlers.Unmarshal.Clear()
	dbc.Handlers.UnmarshalError.Clear()

	return dbc
}

func TestResult(t *testing.T) {
	assert := assert.New(t)
	dbR := &dynamodbResult{}
	dbR.items = []map[string]*dynamodb.AttributeValue{
		map[string]*dynamodb.AttributeValue{
			"number": &dynamodb.AttributeValue{N: util.ConvertString("1")},
			"string": &dynamodb.AttributeValue{S: util.ConvertString("1")},
			"set":    &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&dynamodb.AttributeValue{S: util.ConvertString("s1")}, &dynamodb.AttributeValue{S: util.ConvertString("s2")}}}},
		map[string]*dynamodb.AttributeValue{
			"number": &dynamodb.AttributeValue{N: util.ConvertString("1")},
			"string": &dynamodb.AttributeValue{S: util.ConvertString("1")},
			"set":    &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{&dynamodb.AttributeValue{S: util.ConvertString("s1")}, &dynamodb.AttributeValue{S: util.ConvertString("s2")}}}}}
	assert.Equal(2, dbR.GetItemCount())
	num, ok := dbR.GetNumberItem(0, "number")
	assert.Equal(1, num)
	assert.True(ok)
	ss, ok := dbR.GetStringListItem(0, "set")
	assert.Equal([]string{"s1", "s2"}, ss)
	assert.True(ok)
	s, ok := dbR.GetStringItem(1, "string")
	assert.Equal("1", s)
	assert.True(ok)

	ps := &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
		"asdf": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"t": &dynamodb.AttributeValue{N: util.ConvertString("1")},
			"e": &dynamodb.AttributeValue{N: util.ConvertString("2")},
		}},
		"fdsa": &dynamodb.AttributeValue{M: map[string]*dynamodb.AttributeValue{
			"s": &dynamodb.AttributeValue{N: util.ConvertString("3")},
			"e": &dynamodb.AttributeValue{N: util.ConvertString("4")},
		}}}}

	r := &dynamodbResult{items: []map[string]*dynamodb.AttributeValue{
		map[string]*dynamodb.AttributeValue{"ps": ps}}}

	trns := make(map[string]map[string]int)

	e, _ := r.UnmarshalItem(0, "ps", &trns)
	assert.Nil(e)
	assert.Equal(map[string]map[string]int{
		"asdf": map[string]int{"t": 1, "e": 2},
		"fdsa": map[string]int{"s": 3, "e": 4}}, trns)
}

func TestStartTransaction(t *testing.T) {
	c := &DynamoDBDatastore{transaction: false}
	c.StartTransaction()
	assert.True(t, c.transaction, "Transaction flag not set to true")
	assert.NotNil(t, c.ops, "Did not create operation array")
}

func TestRun(t *testing.T) {
	assert := assert.New(t)

	dbc := getDbClient()

	var opList []string

	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		opList = append(opList, r.Operation.Name)
	})

	c := &DynamoDBDatastore{transaction: false, db: dbc}

	r, e := c.Run(Request{
		Table:  "test",
		Action: Put,
		Key:    map[string]interface{}{"id": "1"},
		Item:   map[string]interface{}{"field1": "value1", "field2": 1}})

	assert.NotNil(r, "Response Nil")
	assert.Nil(e, "Error Nil")

	r, e = c.Run(Request{
		Table:  "test",
		Action: Delete,
		Key:    map[string]interface{}{"id": "1"}})

	assert.NotNil(r, "Response Nil")
	assert.Nil(e, "Error Nil")

	r, e = c.Run(Request{
		Table:  "test",
		Action: Get,
		Key:    map[string]interface{}{"id": "1"}})

	assert.NotNil(r, "Response Nil")
	assert.Nil(e, "Error Nil")

	r, e = c.Run(Request{
		Table:  "test",
		Action: Update,
		Key:    map[string]interface{}{"id": "1"}})

	assert.NotNil(r)
	assert.Nil(e)

	assert.Equal([]string{"PutItem", "DeleteItem", "GetItem", "UpdateItem"}, opList)
}

func TestBuildConditionExpression(t *testing.T) {
	assert := assert.New(t)
	attValMap := make(map[string]*dynamodb.AttributeValue)
	attValNames := make(map[string]*string)

	c := []RequestCondition{RequestCondition{Field: "test1", Type: -1}}

	exp, err := buildConditionExpression(c, attValMap, attValNames)
	assert.Empty(exp)
	if assert.NotNil(err) {
		assert.Equal("Unknown request condition", err.Error())
	}

	c = nil
	c = append(c, RequestCondition{Field: "test2", Type: Exist})
	attValNames = make(map[string]*string)
	exp, err = buildConditionExpression(c, attValMap, attValNames)
	assert.Nil(err)
	if assert.NotEmpty(exp) {
		assert.Equal("attribute_exists(#ename0)", exp)
		assert.Equal(*attValNames["#ename0"], "test2")
		assert.Len(attValMap, 0)
	}

	c = append(c, RequestCondition{Field: "test3", Type: NotExist, Relationship: And})
	attValNames = make(map[string]*string)
	exp, err = buildConditionExpression(c, attValMap, attValNames)
	assert.Nil(err)
	if assert.NotEmpty(exp) {
		assert.Equal("attribute_exists(#ename0) AND attribute_not_exists(#ename1)", exp)
		assert.Equal(*attValNames["#ename0"], "test2")
		assert.Equal(*attValNames["#ename1"], "test3")
		assert.Len(attValMap, 0)
	}

	c = nil

	c = append(c, RequestCondition{Field: "test4", Type: GreaterThan, Value: "string"})
	attValNames = make(map[string]*string)
	exp, err = buildConditionExpression(c, attValMap, attValNames)
	assert.Empty(exp)
	if assert.NotNil(err) {
		assert.Equal("Invalid request condition: GreaterThan condition value must be an int", err.Error())
	}

	c = nil
	c = append(c, RequestCondition{Field: "test4", Type: GreaterThan, Value: 42})
	attValNames = make(map[string]*string)
	exp, err = buildConditionExpression(c, attValMap, attValNames)
	assert.Nil(err)
	if assert.NotEmpty(exp) {
		assert.Equal("#ename0 > :val0", exp)
		assert.Equal(*attValNames["#ename0"], "test4")
		assert.Equal(&dynamodb.AttributeValue{N: util.ConvertString("42")}, attValMap[":val0"])
	}

	c = append(c, RequestCondition{Field: "test5", Type: BeginsWith, Value: "amd", Relationship: And})
	attValMap = make(map[string]*dynamodb.AttributeValue)
	attValNames = make(map[string]*string)
	exp, err = buildConditionExpression(c, attValMap, attValNames)
	assert.Nil(err)
	if assert.NotEmpty(exp) {
		assert.Equal("#ename0 > :val0 AND begins_with(#ename1, :val1)", exp)
		assert.Equal(&dynamodb.AttributeValue{N: util.ConvertString("42")}, attValMap[":val0"])
		assert.Equal(&dynamodb.AttributeValue{S: util.ConvertString("amd")}, attValMap[":val1"])
		assert.Equal(*attValNames["#ename0"], "test4")
		assert.Equal(*attValNames["#ename1"], "test5")
	}
}

func TestBuildUpdateExpression(t *testing.T) {
	assert := assert.New(t)
	expAMap := make(map[string]*dynamodb.AttributeValue)
	expNames := make(map[string]*string)

	i := make([]UpdateValue, 0)

	i = append(i, UpdateValue{Action: Delete, Path: "/test/2"})
	i = append(i, UpdateValue{Action: Put, Path: "/test1", Value: []string{"test1", "test2"}})
	i = append(i, UpdateValue{Action: Update, Path: "/test2/test2", Value: 12345})

	_, err := buildUpdateExpression(i, expAMap, expNames)
	assert.Nil(err)

	i = []UpdateValue{UpdateValue{Action: Put, Path: "/test/2", Value: "test"}}

	_, err = buildUpdateExpression(i, expAMap, expNames)
	assert.Nil(err)

	i = []UpdateValue{UpdateValue{Action: Update, Path: "/test/2", Value: "test"}}

	_, err = buildUpdateExpression(i, expAMap, expNames)
	assert.Nil(err)

	i = []UpdateValue{UpdateValue{Action: Put, Path: "/test/-", Value: []string{"test1", "test2"}}}
	_, err = buildUpdateExpression(i, expAMap, expNames)
	assert.Nil(err)
}

func TestParseUpdateKey(t *testing.T) {
	assert := assert.New(t)

	val, root := parseUpdateKey("/path/in/a/map", 1)
	assert.Equal("#1ename0.#1ename1.#1ename2.#1ename3", val)
	assert.Equal(map[string]string{"#1ename0": "path", "#1ename1": "in", "#1ename2": "a", "#1ename3": "map"}, root)

	val, root = parseUpdateKey("/path/0/in/a/list", 1)
	assert.Equal("#1ename0[0].#1ename2.#1ename3.#1ename4", val)
	assert.Equal(map[string]string{"#1ename0": "path", "#1ename2": "in", "#1ename3": "a", "#1ename4": "list"}, root)

	val, root = parseUpdateKey("/path/0/in/another/3/list", 1)
	assert.Equal("#1ename0[0].#1ename2.#1ename3[3].#1ename5", val)
	assert.Equal(map[string]string{"#1ename0": "path", "#1ename2": "in", "#1ename3": "another", "#1ename5": "list"}, root)
}

func TestMarshalItem(t *testing.T) {
	assert := assert.New(t)

	in := map[string]interface{}{"key1": "ValueString", "Key2": 2, "Key3": []string{"three1", "three2"}}
	r, e := marshalItems(in)
	assert.Nil(e)
	if assert.NotNil(r) {
		assert.Equal(&dynamodb.AttributeValue{S: util.ConvertString("ValueString")}, r["key1"])
		assert.Equal(&dynamodb.AttributeValue{N: util.ConvertString("2")}, r["Key2"])
		assert.Equal(&dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			&dynamodb.AttributeValue{S: util.ConvertString("three1")},
			&dynamodb.AttributeValue{S: util.ConvertString("three2")}}}, r["Key3"])
	}

}

func TestUnmarshalItem(t *testing.T) {
	in := map[string]*dynamodb.AttributeValue{
		"field1": &dynamodb.AttributeValue{S: util.ConvertString("value1 string")},
		"field2": &dynamodb.AttributeValue{N: util.ConvertString("2")},
		"field3": &dynamodb.AttributeValue{SS: []*string{util.ConvertString("value3.1"), util.ConvertString("value3.2")}},
		"field4": &dynamodb.AttributeValue{L: []*dynamodb.AttributeValue{
			&dynamodb.AttributeValue{S: util.ConvertString("four1")},
			&dynamodb.AttributeValue{S: util.ConvertString("four2")}}}}

	out := unmarshalItems(in)
	assert.Equal(t, map[string]interface{}{
		"field1": "value1 string",
		"field2": float64(2),
		"field3": []string{"value3.1", "value3.2"},
		"field4": []interface{}{"four1", "four2"}}, out)
}

func TestPut(t *testing.T) {
	assert := assert.New(t)

	r := Request{
		Table:  "test",
		Action: Put,
		Key:    map[string]interface{}{"id": "1"},
		Item:   map[string]interface{}{"field1": "value1", "field2": 1}}

	dbc := getDbClient()
	dbc.Handlers.Send.Clear()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		p, ok := r.Params.(*dynamodb.PutItemInput)
		if assert.True(ok) {
			assert.Equal("1", *p.Item["id"].S)
			assert.Equal("value1", *p.Item["field1"].S)
			assert.Equal("1", *p.Item["field2"].N)
		}
	})

	dbr, e := put(dbc, r)

	assert.NotNil(dbr, "Response Nil")
	assert.Nil(e, "Error Nil")

	dbc.Handlers.Send.Clear()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = awserr.New("TestError", "testing", nil)
		r.Retryable = util.ConvertBool(false)
	})

	dbr, e = put(dbc, r)

	assert.Nil(dbr, "Response Nil")
	if assert.NotNil(e, "Error Nil") {
		assert.Equal(ErrorPutItem, e)
	}

}

func TestUpdate(t *testing.T) {
	assert := assert.New(t)

	r := Request{
		Table:  EntityTable,
		Action: Update,
		Key:    map[string]interface{}{"id": "usr0e425603f00d0737642693e3bd5e4432"}}

	r.Updates = []UpdateValue{UpdateValue{Action: Update, Path: "/name", Value: "Test"}}

	dbc := getDbClient()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		p, ok := r.Params.(*dynamodb.UpdateItemInput)
		if assert.True(ok) {
			assert.Equal(map[string]*dynamodb.AttributeValue{":val0": &dynamodb.AttributeValue{S: util.ConvertString("Test")}}, p.ExpressionAttributeValues)
			assert.Equal("SET #0ename0 = :val0", *p.UpdateExpression)
			assert.Equal(map[string]*string{"#0ename0": util.ConvertString("name")}, p.ExpressionAttributeNames)
			assert.Equal(map[string]*dynamodb.AttributeValue{
				"id": &dynamodb.AttributeValue{S: util.ConvertString("usr0e425603f00d0737642693e3bd5e4432")}}, p.Key)
		}
	})

	dbr, e := update(dbc, r)
	assert.Nil(e)
	assert.NotNil(dbr)

	dbc.Handlers.Send.Clear()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = awserr.New("TestError", "testing", nil)
		r.Retryable = util.ConvertBool(false)
	})

	dbr, e = update(dbc, r)
	assert.Nil(dbr, "Response Nil")
	if assert.NotNil(e, "Error Nil") {
		assert.Equal(ErrorUpdateItem, e)
	}
}

func TestQuery(t *testing.T) {
	assert := assert.New(t)

	r := Request{
		Table:  "test",
		Action: Query,
		RequestConditions: []RequestCondition{
			RequestCondition{Field: "test", Type: Equal, Value: "equal"}}}

	dbc := getDbClient()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		p, ok := r.Params.(*dynamodb.QueryInput)
		if assert.True(ok) {
			assert.Equal("#ename0 = :val0", *p.KeyConditionExpression)
			assert.Equal(map[string]*string{"#ename0": util.ConvertString("test")}, p.ExpressionAttributeNames)
			assert.Equal(map[string]*dynamodb.AttributeValue{":val0": &dynamodb.AttributeValue{S: util.ConvertString("equal")}}, p.ExpressionAttributeValues)
		}
		data := r.Data.(*dynamodb.QueryOutput)
		data.Items = []map[string]*dynamodb.AttributeValue{
			map[string]*dynamodb.AttributeValue{
				"t1":   &dynamodb.AttributeValue{S: util.ConvertString("t1v")},
				"t1.2": &dynamodb.AttributeValue{N: util.ConvertString("2")}},
			map[string]*dynamodb.AttributeValue{
				"t2":   &dynamodb.AttributeValue{S: util.ConvertString("t2v")},
				"t2.2": &dynamodb.AttributeValue{N: util.ConvertString("4")}}}
		data.Count = util.ConverInt64(2)
	})

	dbr, e := query(dbc, r)
	assert.Nil(e)
	if assert.NotNil(dbr) {
		if assert.Equal(2, dbr.GetItemCount()) {
			st, _ := dbr.GetStringItem(0, "t1")
			assert.Equal("t1v", st)
			n, _ := dbr.GetNumberItem(0, "t1.2")
			assert.Equal(2, n)
			st, _ = dbr.GetStringItem(1, "t2")
			assert.Equal("t2v", st)
			n, _ = dbr.GetNumberItem(1, "t2.2")
			assert.Equal(4, n)
		}
	}

	dbc.Handlers.Send.Clear()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = awserr.New("TestError", "testing", nil)
		r.Retryable = util.ConvertBool(false)
	})

	dbr, e = query(dbc, r)
	assert.Nil(dbr, "Response Nil")
	if assert.NotNil(e, "Error Nil") {
		assert.Equal(ErrorQueryItem, e)
	}
}

func TestQueryPages(t *testing.T) {
	assert := assert.New(t)

	r := Request{
		Table:    "test",
		Action:   Query,
		Page:     1,
		PageSize: 10,
		RequestConditions: []RequestCondition{
			RequestCondition{Field: "test", Type: Equal, Value: "equal"}}}

	pages := 0
	dbc := getDbClient()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		p, ok := r.Params.(*dynamodb.QueryInput)
		if assert.True(ok) {
			assert.Equal("#ename0 = :val0", *p.KeyConditionExpression)
			assert.Equal(map[string]*string{"#ename0": util.ConvertString("test")}, p.ExpressionAttributeNames)
			assert.Equal(map[string]*dynamodb.AttributeValue{":val0": &dynamodb.AttributeValue{S: util.ConvertString("equal")}}, p.ExpressionAttributeValues)
		}
		pstr := "page" + strconv.Itoa(pages)
		data := r.Data.(*dynamodb.QueryOutput)
		data.Items = []map[string]*dynamodb.AttributeValue{
			map[string]*dynamodb.AttributeValue{
				pstr + "t1":   &dynamodb.AttributeValue{S: util.ConvertString("t1v")},
				pstr + "t1.2": &dynamodb.AttributeValue{N: util.ConvertString("2")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t2":   &dynamodb.AttributeValue{S: util.ConvertString("t2v")},
				pstr + "t2.2": &dynamodb.AttributeValue{N: util.ConvertString("4")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t3":   &dynamodb.AttributeValue{S: util.ConvertString("t3v")},
				pstr + "t3.2": &dynamodb.AttributeValue{N: util.ConvertString("6")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t4":   &dynamodb.AttributeValue{S: util.ConvertString("t4v")},
				pstr + "t4.2": &dynamodb.AttributeValue{N: util.ConvertString("8")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t5":   &dynamodb.AttributeValue{S: util.ConvertString("t5v")},
				pstr + "t5.2": &dynamodb.AttributeValue{N: util.ConvertString("10")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t6":   &dynamodb.AttributeValue{S: util.ConvertString("t6v")},
				pstr + "t6.2": &dynamodb.AttributeValue{N: util.ConvertString("12")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t7":   &dynamodb.AttributeValue{S: util.ConvertString("t7v")},
				pstr + "t7.2": &dynamodb.AttributeValue{N: util.ConvertString("14")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t8":   &dynamodb.AttributeValue{S: util.ConvertString("t8v")},
				pstr + "t8.2": &dynamodb.AttributeValue{N: util.ConvertString("16")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t9":   &dynamodb.AttributeValue{S: util.ConvertString("t9v")},
				pstr + "t9.2": &dynamodb.AttributeValue{N: util.ConvertString("18")}},
			map[string]*dynamodb.AttributeValue{
				pstr + "t10":   &dynamodb.AttributeValue{S: util.ConvertString("t10v")},
				pstr + "t10.2": &dynamodb.AttributeValue{N: util.ConvertString("20")}}}
		data.Count = util.ConverInt64(10)
		if pages < 3 {
			data.LastEvaluatedKey = map[string]*dynamodb.AttributeValue{
				"t9": &dynamodb.AttributeValue{S: util.ConvertString("t10v")}}
			pages++
		}
	})

	dbr, e := queryPages(dbc, r)
	assert.Nil(e)
	if assert.NotNil(dbr.items) && assert.Len(dbr.items, 10) {
		for i := 0; i < 10; i++ {
			tval := "t" + strconv.Itoa(i+1)
			item := dbr.items[i]
			assert.NotNil(item["page0"+tval], "looking for: page0"+tval)
			assert.NotNil(item["page0"+tval+".2"], "looking for: page0"+tval+".2")
		}
		assert.Equal(4, dbr.PageCount())
	}

	//reset the counter
	pages = 0

	//get the second page
	r.Page = 2
	dbr, e = queryPages(dbc, r)
	assert.Nil(e)
	if assert.NotNil(dbr.items) && assert.Len(dbr.items, 10) {
		for i := 0; i < 10; i++ {
			tval := "t" + strconv.Itoa(i+1)
			item := dbr.items[i]
			assert.NotNil(item["page1"+tval], "looking for: page1"+tval)
			assert.NotNil(item["page1"+tval+".2"], "looking for: page1"+tval+".2")
		}
		assert.Equal(4, dbr.PageCount())
	}

	//reset the counter
	pages = 0

	//get a different page
	r.Page = 3
	r.PageSize = 3
	dbr, e = queryPages(dbc, r)
	assert.Nil(e)
	if assert.NotNil(dbr.items) && assert.Len(dbr.items, 3) {
		assert.NotNil(dbr.items[0]["page0t7"], "looking for: page1t7")
		assert.NotNil(dbr.items[1]["page0t8"], "looking for: page1t8")
		assert.NotNil(dbr.items[2]["page0t9"], "looking for: page1t9")
		assert.Equal(14, dbr.PageCount())
	}
}

func TestDelete(t *testing.T) {
	assert := assert.New(t)

	r := Request{
		Table:  "test",
		Action: Delete,
		Key:    map[string]interface{}{"id": "usr0e425603f00d0737642693e3bd5e4432"}}

	dbc := getDbClient()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		p, ok := r.Params.(*dynamodb.DeleteItemInput)
		if assert.True(ok) {
			assert.Equal(map[string]*dynamodb.AttributeValue{
				"id": &dynamodb.AttributeValue{S: util.ConvertString("usr0e425603f00d0737642693e3bd5e4432")}}, p.Key)
		}
	})

	dbr, e := dbDelete(dbc, r)

	assert.NotNil(dbr, "Response Nil")
	assert.Nil(e, "Error Nil")

	dbc.Handlers.Send.Clear()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = awserr.New("TestError", "testing", nil)
		r.Retryable = util.ConvertBool(false)
	})

	dbr, e = dbDelete(dbc, r)
	assert.Nil(dbr, "Response Nil")
	if assert.NotNil(e, "Error Nil") {
		assert.Equal(ErrorDeleteItem, e)
	}
}

func TestGet(t *testing.T) {
	assert := assert.New(t)

	r := Request{
		Table:  "test",
		Action: Get,
		Key:    map[string]interface{}{"id": "usr0e425603f00d0737642693e3bd5e4432"}}

	dbc := getDbClient()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		p, ok := r.Params.(*dynamodb.GetItemInput)
		if assert.True(ok) {
			assert.Equal(map[string]*dynamodb.AttributeValue{
				"id": &dynamodb.AttributeValue{S: util.ConvertString("usr0e425603f00d0737642693e3bd5e4432")}}, p.Key)
		}
		data := r.Data.(*dynamodb.GetItemOutput)
		data.Item = map[string]*dynamodb.AttributeValue{
			"one": &dynamodb.AttributeValue{S: util.ConvertString("onevalue")},
			"two": &dynamodb.AttributeValue{S: util.ConvertString("twovalue")}}
	})

	dbr, e := get(dbc, r)

	assert.NotNil(dbr, "Response nil")
	assert.Nil(e, "Error nil")

	if assert.NotNil(dbr.items, "Items should not be nil") {
		assert.Len(dbr.items, 1)
		assert.Equal(dbr.items[0], map[string]*dynamodb.AttributeValue{
			"one": &dynamodb.AttributeValue{S: util.ConvertString("onevalue")},
			"two": &dynamodb.AttributeValue{S: util.ConvertString("twovalue")}})
	}

	dbc.Handlers.Send.Clear()
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		r.Error = awserr.New("TestError", "testing", nil)
		r.Retryable = util.ConvertBool(false)
	})

	dbr, e = get(dbc, r)
	assert.Nil(dbr, "Response Nil")
	if assert.NotNil(e, "Error Nil") {
		assert.Equal(ErrorGetItem, e)
	}
}

func TestReverseOp(t *testing.T) {
	assert := assert.New(t)
	var o op
	o.request = Request{
		Table:  "test",
		Action: Put,
		Key:    map[string]interface{}{"id": "valueofid"},
		Item:   map[string]interface{}{"field1": "Val1", "field2": 2}}

	r := reverseOp(o)
	assert.Equal(r.Action, Delete, "Wrong reversed action")
	assert.Equal(r.Key, o.request.Key, "Reverse key not preserved")

	o.request.Action = Delete
	o.request.Item = nil
	o.result = &dynamodbResult{attributes: map[string]*dynamodb.AttributeValue{
		"field1": &dynamodb.AttributeValue{S: util.ConvertString("value1 string")},
		"field2": &dynamodb.AttributeValue{N: util.ConvertString("2")},
		"field3": &dynamodb.AttributeValue{SS: []*string{util.ConvertString("value3.1"), util.ConvertString("value3.2")}}}}

	r = reverseOp(o)
	if assert.Equal(r.Action, Put) {
		assert.Equal(map[string]interface{}{
			"field1": "value1 string",
			"field2": float64(2),
			"field3": []string{"value3.1", "value3.2"}}, r.Item)
	}

	o.request.Action = Update
	o.request.Updates = []UpdateValue{
		UpdateValue{Delete, "field1", nil},
		UpdateValue{Put, "field2", 3},
		UpdateValue{Put, "field4", "irrelevant, should delete"}}

	r = reverseOp(o)
	if assert.Equal(r.Action, Update) {
		assert.Equal([]UpdateValue{
			UpdateValue{Put, "field1", "value1 string"},
			UpdateValue{Put, "field2", float64(2)},
			UpdateValue{Delete, "", nil}}, r.Updates)
	}

}

func TestRollback(t *testing.T) {
	assert := assert.New(t)
	dbc := getDbClient()
	var opList []string
	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		opList = append(opList, r.Operation.Name)
	})
	c := &DynamoDBDatastore{
		db: dbc,
		ops: []op{
			op{
				Request{
					Action: Put,
					Key:    map[string]interface{}{"id": "1234"},
					Table:  "test"},
				&dynamodbResult{}},
			op{
				Request{
					Action: Update,
					Key:    map[string]interface{}{"id": "1234"},
					Table:  "test"},
				&dynamodbResult{attributes: map[string]*dynamodb.AttributeValue{
					"field1": &dynamodb.AttributeValue{S: util.ConvertString("value1 string")},
					"field2": &dynamodb.AttributeValue{N: util.ConvertString("2")},
					"field3": &dynamodb.AttributeValue{SS: []*string{util.ConvertString("value3.1"), util.ConvertString("value3.2")}}}}},
			op{
				Request{
					Action: Delete,
					Key:    map[string]interface{}{"id": "1234"},
					Table:  "test"},
				&dynamodbResult{attributes: map[string]*dynamodb.AttributeValue{
					"field1": &dynamodb.AttributeValue{S: util.ConvertString("value1 string")},
					"field2": &dynamodb.AttributeValue{N: util.ConvertString("2")},
					"field3": &dynamodb.AttributeValue{SS: []*string{util.ConvertString("value3.1"), util.ConvertString("value3.2")}}}}}}}
	err := c.Rollback()
	assert.Nil(err)
	assert.Equal([]string{"DeleteItem", "UpdateItem", "PutItem"}, opList)
}

func TestQueryIndex(t *testing.T) {
	/*
			qi := &dynamodb.QueryInput{TableName: util.ConvertString(ItemsTable)}
			qi.IndexName = util.ConvertString("start")

			ke := ItemsFieldID + " = :val0 AND #start < :val1"
			qi.KeyConditionExpression = util.ConvertString(ke)

			kVal := map[string]*dynamodb.AttributeValue{
				":val0": &dynamodb.AttributeValue{S: util.ConvertString("subc04f2fb522359b3c9269c0495db60f1f.banners")},
				":val1": &dynamodb.AttributeValue{N: util.ConvertString("3460572346")}}
			kEName := map[string]*string{"#start": util.ConvertString("start")}

			qi.ExpressionAttributeValues = kVal
			qi.ExpressionAttributeNames = kEName
			//db := dynamodb.New(session.New(), aws.NewConfig().WithRegion("us-west-2"))

			qo, e := db.Query(qi)

			if e != nil {
				fmt.Printf("Errorr: %v\n\n", e)
			} else {
				fmt.Printf("Items: %+v\n\n", qo.Items)
			}
		d := NewDynamoDb()
		req := Request{
			Table:  ItemsTable,
			Action: Query,
			Index:  "start"}
		req.AddCondition("start", LessThan, And, 10)
		req.AddCondition("id", Equal, And, "subc04f2fb522359b3c9269c0495db60f1f.banners")
		req.AddFilter("end", GreaterThan, And, 20)
		req.AddFilter("active", NotExist, And, nil)

		resp, err := d.Run(req)
		if err != nil {
			fmt.Printf("%v\n\n", err.JSON())
		} else {
			fmt.Printf("%v\n\n", resp.GetOriginalItem())
		}
	*/
}

func TestCacheHit(t *testing.T) {

	assert := assert.New(t)
	dbc := getDbClient()

	r := Request{
		Table:  "test",
		Action: Get,
		Key:    map[string]interface{}{"id": "usr0e425603f00d0737642693e3bd5e4432", "subd": 1234}}

	var opList []string

	dbc.Handlers.Send.PushBack(func(r *request.Request) {
		opList = append(opList, r.Operation.Name)
		if len(opList) == 1 {
			data := r.Data.(*dynamodb.GetItemOutput)
			data.Item = map[string]*dynamodb.AttributeValue{
				"one": &dynamodb.AttributeValue{S: util.ConvertString("onevalue")},
				"two": &dynamodb.AttributeValue{S: util.ConvertString("twovalue")}}
		} else {
			data := r.Data.(*dynamodb.GetItemOutput)
			data.Item = map[string]*dynamodb.AttributeValue{
				"three": &dynamodb.AttributeValue{S: util.ConvertString("threevalue")},
				"four":  &dynamodb.AttributeValue{S: util.ConvertString("fourvalue")}}
		}

	})

	c := &DynamoDBDatastore{db: dbc}

	res, e := c.Run(r)
	assert.Nil(e)
	assert.NotNil(res)
	assert.Len(opList, 1)

	res2, e := c.Run(r)
	assert.Nil(e)
	assert.NotNil(res2)
	assert.Len(opList, 1)
	assert.Equal(res, res2)

	r.Key = map[string]interface{}{"id": "usr0e425603f00d0737642693e3bd5e4432"}

	res3, e := c.Run(r)
	assert.Nil(e)
	assert.NotNil(res3)
	assert.NotEqual(res, res3)
	assert.Len(opList, 2)
}
