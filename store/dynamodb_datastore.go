package store

import (
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/sethjback/godba/config"
)

// DBer is a subset of the dynamoDB interface and is
// used so we can mock for testing
type DBer interface {
	GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	QueryPages(input *dynamodb.QueryInput, fn func(p *dynamodb.QueryOutput, lastPage bool) bool) error
	Query(*dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
}

// make sure we implement the interface
var _ DBer = (*dynamodb.DynamoDB)(nil)

// DynamoDBDatastore implements the datastore interface
// It holds a dynamodb DBer and manages db ops and rollbacks
type DynamoDBDatastore struct {
	db            DBer
	ops           []op
	transaction   bool
	cache         []op
	tablePrefix   string
	cacheDisabled bool
}

// Individual operation performed in dynamodb. Used for rollbacks
type op struct {
	request Request
	result  *dynamodbResult
}

// a result from a sucessful dynamodb operation.
// stores the original response, as well as any returned Items
// attributes stores the original values if an update was performed: used for rollback
type dynamodbResult struct {
	items      []map[string]*dynamodb.AttributeValue
	attributes map[string]*dynamodb.AttributeValue
	pageCount  int
}

/**

Implements the result interface

**/

// GetStringItem returns a string dynamodb value
func (r *dynamodbResult) GetStringItem(itemIndex int, name string) (string, bool) {

	if v, ok := r.items[itemIndex][name]; ok && v.S != nil {
		return *v.S, true
	}

	return "", false
}

// GetNumberItem returns a number value (converts to int)
func (r *dynamodbResult) GetNumberItem(itemIndex int, name string) (int, bool) {
	v := -1

	if r.items[itemIndex][name] == nil || r.items[itemIndex][name].N == nil {
		return v, false
	}

	if i, ok := strconv.Atoi(*r.items[itemIndex][name].N); ok == nil {
		return i, true
	}

	return v, false
}

// GetStringListItem converts a string list to []string
func (r *dynamodbResult) GetStringListItem(itemIndex int, name string) ([]string, bool) {
	var ss []string

	if r.items[itemIndex][name] == nil || r.items[itemIndex][name].L == nil {
		return ss, false
	}

	for _, s := range r.items[itemIndex][name].L {
		ss = append(ss, *s.S)
	}

	return ss, true
}

// GetBoolItem returns the bool value
func (r *dynamodbResult) GetBoolItem(itemIndex int, name string) (bool, bool) {
	if r.items[itemIndex][name] == nil || r.items[itemIndex][name].BOOL == nil {
		return false, false
	}

	return *r.items[itemIndex][name].BOOL, true
}

// GetItemCount returns the number of items retrieved from the DB
func (r *dynamodbResult) GetItemCount() int {
	return len(r.items)
}

// UnmarshalItem attempts to unmarshal a dynamoDB attributevalue into the out interface
func (r *dynamodbResult) UnmarshalItem(itemIndex int, name string, out interface{}) (error, bool) {
	v, ok := r.items[itemIndex][name]
	if !ok {
		return nil, false
	}
	e := dynamodbattribute.Unmarshal(v, out)
	if e != nil {
		return errors.New("Could not unmarshal item" + "[" + e.Error() + "]"), false
	}
	return nil, true
}

// GetItem returns a raw item
func (r *dynamodbResult) GetItem(itemIndex int, name string) (interface{}, bool) {
	i, ok := r.items[itemIndex][name]
	if ok {
		return i, true
	}
	return nil, false
}

func (r *dynamodbResult) GetLastEvaluatedKey() map[string]interface{} {
	/*
		if qO, ok := r.original.(*dynamodb.QueryOutput); ok {
			m := make(map[string]interface{})
			for k, v := range qO.LastEvaluatedKey {
				if v.S != nil {
					m[k] = *v.S
				}
				if v.N != nil {
					m[k], _ = strconv.Atoi(*v.N)
				}
			}

			return m
		}
	*/
	return nil
}

func (r *dynamodbResult) PageCount() int {
	return r.pageCount
}

/*

Configuration options

*/

const (
	Session config.Option = iota
	Endpoint
	TablePrefix
)

func NewDynamodb(c config.Store) *DynamoDBDatastore {
	dbc := &DynamoDBDatastore{transaction: false}

	var sess *session.Session

	ses, ok := c.Get(Session)
	if !ok {
		sess = session.New()
	} else {
		sess = ses.(*session.Session)
	}

	dbe := c.GetString(Endpoint)

	dbc.db = dynamodb.New(sess, &aws.Config{Endpoint: aws.String(dbe)})

	dbc.tablePrefix = c.GetString(TablePrefix)

	return dbc
}

/**

Implements the DataStore interface

**/

// clears the result cache
func (c *DynamoDBDatastore) ClearCache() {
	c.cache = nil
	c.ops = nil
}

func (c *DynamoDBDatastore) CacheOn() {
	c.cacheDisabled = false
}

func (c *DynamoDBDatastore) CacheOff() {
	c.cacheDisabled = true
}

// Run runs a single reqeust operation on the DB
func (c *DynamoDBDatastore) Run(request Request) (Result, error) {

	var r *dynamodbResult
	var e error

	request.Table = c.tablePrefix + request.Table

	switch request.Action {
	case Put:
		r, e = put(c.db, request)
	case Delete:
		if c.transaction {
			request.ReturnValues = "ALL_OLD"
		}
		r, e = dbDelete(c.db, request)
	case Get:
		//check if we've already done this
		if !request.LiveData && !c.cacheDisabled {
			for _, op := range c.cache {
				if keysEqual(op.request.Key, request.Key) {
					return op.result, nil
				}
			}
		}
		r, e = get(c.db, request)
		if e == nil {
			c.cache = append(c.cache, op{request, r})
		}
	case Update:
		if c.transaction {
			request.ReturnValues = "ALL_OLD"
		}
		r, e = update(c.db, request)
	case Query:
		r, e = query(c.db, request)
	case QueryPager:
		r, e = queryPages(c.db, request)
	}

	if c.transaction && e == nil {
		c.ops = append(c.ops, op{request, r})
	}

	return r, e
}

func keysEqual(k1 map[string]interface{}, k2 map[string]interface{}) bool {
	if len(k1) != len(k2) {
		return false
	}

	for i, v := range k1 {
		if k2[i] == nil || k2[i] != v {
			return false
		}
	}

	return true
}

// StartTransaction initializes the client to record multiple operations, which can be rolled back later
func (c *DynamoDBDatastore) StartTransaction() {
	c.transaction = true
	c.ops = make([]op, 0)
}

func (c *DynamoDBDatastore) FinishTransaction() {
	c.transaction = false
	c.ops = nil
}

// Rollback runs through successfully completed requests and reverses them
// If there are any errors when performing the reversing function, they are returned
func (c *DynamoDBDatastore) Rollback() []error {
	c.transaction = false
	var errs []error
	for _, op := range c.ops {
		reverse := reverseOp(op)
		_, e := c.Run(*reverse)
		if e != nil {
			errs = append(errs, e)
		}
	}

	return errs
}

// buildConditionExpression takes a slice of conditions and builds the appropriate expression. Can be used
// anywhere a condition expression is needed in dynamodb (filters, keyconditions, etc.)
func buildConditionExpression(conditions []RequestCondition, expAttVals map[string]*dynamodb.AttributeValue, expAttNames map[string]*string) (string, error) {

	exp := ""
	valI := len(expAttVals)
	valN := len(expAttNames)
	for i, c := range conditions {
		if exp != "" {
			exp += " "
		}

		if i > 0 && c.Relationship != -1 {
			exp += c.RelationshipString() + " "
		}

		fName := "#ename" + strconv.Itoa(valN)
		expAttNames[fName] = aws.String(c.Field)
		valN++

		switch c.Type {
		case Exist:
			exp += "attribute_exists(" + fName + ")"
		case NotExist:
			exp += "attribute_not_exists(" + fName + ")"
		case GreaterThan:
			if i, ok := c.Value.(int); ok {
				exp += fName + " > :val" + strconv.Itoa(valI)
				expAttVals[":val"+strconv.Itoa(valI)] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(i))}
				valI++
			} else {
				return "", errors.New("Invalid request condition: GreaterThan condition value must be an int")
			}
		case LessThan:
			if i, ok := c.Value.(int); ok {
				exp += fName + " < :val" + strconv.Itoa(valI)
				expAttVals[":val"+strconv.Itoa(valI)] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(i))}
				valI++
			} else {
				return "", errors.New("Invalid request condition: LessThan condition value must be an int")
			}
		case Equal:
			exp += fName + " = :val" + strconv.Itoa(valI)
			switch reflect.TypeOf(c.Value).Name() {
			case "int":
				expAttVals[":val"+strconv.Itoa(valI)] = &dynamodb.AttributeValue{N: aws.String(strconv.Itoa(c.Value.(int)))}
			case "string":
				expAttVals[":val"+strconv.Itoa(valI)] = &dynamodb.AttributeValue{S: aws.String(c.Value.(string))}
			default:
				return "", errors.New("Invalid request condition: Equal condition value must be int or string")
			}
			valI++
		case BeginsWith:
			if st, ok := c.Value.(string); ok {
				exp += "begins_with(" + fName + ", :val" + strconv.Itoa(valI) + ")"
				expAttVals[":val"+strconv.Itoa(valI)] = &dynamodb.AttributeValue{S: aws.String(st)}
				valI++
			} else {
				return "", errors.New("Invalid request condition: BeginsWith condition value must be a string")
			}
		default:
			return "", errors.New("Unknown request condition")
		}
	}

	return exp, nil
}

// translates the path from an rfc6901 path into a dynamodb update expression path
// the only major difference is that list items are not referenece by a delimiter, but rather
// by the array type notation (i.e. list[1] vs. list.1)
// the rootName is the value the root should be translated to. For dynamodb, this will be the actual
// attribute name, and given there are a slew of protected names we need to translate this to a #val
// replacement
func parseUpdateKey(key string, index int) (string, map[string]string) {
	segs := strings.Split(key, "/")[1:]
	names := make(map[string]string)
	iString := strconv.Itoa(index)
	final := ""
	var ename string

	for i, val := range segs {
		_, e := strconv.Atoi(val)
		if e == nil {
			final += "[" + val + "]"
		} else {

			if val == "-" {
				ename = "-"
			} else {
				ename = "#" + iString + "ename" + strconv.Itoa(i)
				names[ename] = val
			}

			if final == "" {
				final += ename
			} else {
				final += "." + ename
			}
		}
	}

	return final, names
}

// buildUpdateExpression takes a map of UpdateValues (update operations to be performed) and translates them
// into a dynamodb update expression
// It uses :val placeholders for the actual values and indexes them in the expAttMap
func buildUpdateExpression(items []UpdateValue, expAttMap map[string]*dynamodb.AttributeValue, expAttName map[string]*string) (string, error) {
	expMap := map[string]string{"SET": "", "REMOVE": ""}

	i := len(expAttMap)
	n := len(expAttName)
	for _, v := range items {
		valStr := ":val" + strconv.Itoa(i)

		k, nVals := parseUpdateKey(v.Path, n)
		for nName, nVal := range nVals {
			expAttName[nName] = aws.String(nVal)
		}
		n++

		switch v.Action {
		case Delete:
			if expMap["REMOVE"] != "" {
				expMap["REMOVE"] += ", "
			}
			expMap["REMOVE"] += k
		case Put, Update:
			val, err := marshalItems(map[string]interface{}{k: v.Value})
			if err != nil {
				return "", errors.New("Unable to marshal item: " + err.Error())
			}
			// for all else use set
			if expMap["SET"] != "" {
				expMap["SET"] += ", "
			}

			last := k[len(k)-1:]
			// if that last part of the path is - this is an array update
			switch last {
			case "-":
				//TODO: this will fail if list >= 999*
				nK := k[:len(k)-2] + "[999" + strconv.Itoa(n) + "]"
				expMap["SET"] += nK + " = " + valStr
			default:
				expMap["SET"] += k + " = " + valStr
			}
			expAttMap[valStr] = val[k]
		}

		i++
	}

	finalExp := ""
	if expMap["SET"] != "" {
		finalExp += "SET " + expMap["SET"]
	}
	if expMap["REMOVE"] != "" {
		if finalExp != "" {
			finalExp += " "
		}
		finalExp += "REMOVE " + expMap["REMOVE"]
	}

	return finalExp, nil
}

// marshalItems takes a map of attribute names and values, and converts the value into
// an appropriate dynamodb AttributeValue
// In the case of an empty Map or List, want to create empty {} or [] in DynamoDB
// The default dynamodbattribute Marshaler will insert NULL AttributeValues, so we override in those cases
// We also don't want to encode empty strings, so ignore those
func marshalItems(in map[string]interface{}) (map[string]*dynamodb.AttributeValue, error) {
	i := map[string]*dynamodb.AttributeValue{}
	enc := dynamodbattribute.NewEncoder()
	for k, v := range in {
		switch v.(type) {
		case string:
			if len(v.(string)) == 0 {
				continue
			}
		}

		v, err := enc.Encode(v)
		if err != nil {
			return nil, errors.New("Could not marshal item: " + err.Error())
		}
		i[k] = v

	}

	return i, nil
}

// unmarshalItems takes a map of AttributeValues, converts them to native types, and returns the value
// without the AttributeValue struct
func unmarshalItems(in map[string]*dynamodb.AttributeValue) map[string]interface{} {

	vals := make(map[string]interface{})

	for k, v := range in {
		var o interface{}
		e := dynamodbattribute.Unmarshal(v, &o)
		if e != nil {
			return nil
		}
		vals[k] = o
	}
	return vals
}

func put(db DBer, r Request) (*dynamodbResult, error) {
	condexp := ""
	expValMap := make(map[string]*dynamodb.AttributeValue)
	expNameMap := make(map[string]*string)

	// make sure the key is part of the item
	for k, v := range r.Key {
		r.Item[k] = v
	}

	item, err := marshalItems(r.Item)
	if err != nil {
		return nil, errors.New("Could not put item in the db [" + err.Error() + "]")
	}

	if r.RequestConditions != nil {
		condexp, err = buildConditionExpression(r.RequestConditions, expValMap, expNameMap)
		if err != nil {
			return nil, errors.New("Could not put item in the db [" + err.Error() + "]")
		}
	}

	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(r.Table),
		Item:      item}

	if len(condexp) != 0 {
		putInput.ConditionExpression = aws.String(condexp)
	}

	if len(expValMap) != 0 {
		putInput.ExpressionAttributeValues = expValMap
	}

	if len(expNameMap) != 0 {
		putInput.ExpressionAttributeNames = expNameMap
	}

	_, e := db.PutItem(putInput)

	if e != nil {
		var re error
		if awsErr, ok := e.(awserr.Error); ok {
			re = errors.New("Unable to put item in the database [" + awsErr.Error() + "]")
		} else {
			re = errors.New("Unable to put item in the database")
		}
		return nil, re
	}

	return &dynamodbResult{}, nil
}

func get(db DBer, r Request) (*dynamodbResult, error) {
	key, err := marshalItems(r.Key)
	if err != nil {
		return nil, errors.New("Could not get item [" + err.Error() + "]")
	}

	dbResult, e := db.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(r.Table),
		Key:            key,
		ConsistentRead: aws.Bool(r.ConsistentRead)})

	if e != nil {
		var re error
		if awsErr, ok := e.(awserr.Error); ok {
			re = errors.New("Unable to retrieve item from the database [" + awsErr.Error() + "]")
		} else {
			re = errors.New("Unable to retrieve item from the database ")
		}

		return nil, re
	}

	result := &dynamodbResult{}

	if len(dbResult.Item) != 0 {
		result.items = append(result.items, dbResult.Item)
	}

	return result, nil
}

func dbDelete(db DBer, r Request) (*dynamodbResult, error) {
	key, err := marshalItems(r.Key)
	if err != nil {
		return nil, errors.New("Could not delete item [" + err.Error() + "]")
	}

	var returnvals *string
	if r.ReturnValues != "" {
		returnvals = aws.String(r.ReturnValues)
	}

	dbResult, e := db.DeleteItem(&dynamodb.DeleteItemInput{
		TableName:    aws.String(r.Table),
		Key:          key,
		ReturnValues: returnvals})

	if e != nil {
		var re error
		if awsErr, ok := e.(awserr.Error); ok {
			re = errors.New("Unable to delete item in the database [" + awsErr.Error() + "]")
		} else {
			re = errors.New("Unable to delete item in the database")
		}

		return nil, re
	}

	result := &dynamodbResult{}
	if dbResult.Attributes != nil {
		result.attributes = dbResult.Attributes
	}

	return result, nil
}

// update implements the update logic for dynamodb
// it translates the Updates in the request into an update expression and expression value map
// then sends that to dynamodb
func update(db DBer, r Request) (*dynamodbResult, error) {
	key, err := marshalItems(r.Key)
	if err != nil {
		return nil, errors.New("Could not update item [" + err.Error() + "]")
	}

	updateMap := make(map[string]*dynamodb.AttributeValue)
	updateNames := make(map[string]*string)

	updateExp, err := buildUpdateExpression(r.Updates, updateMap, updateNames)
	if err != nil {
		return nil, errors.New("Could not update item [" + err.Error() + "]")
	}

	if len(updateMap) == 0 {
		updateMap = nil
	}
	if len(updateNames) == 0 {
		updateNames = nil
	}

	var returnvals *string
	if r.ReturnValues != "" {
		returnvals = aws.String(r.ReturnValues)
	}

	dbResult, e := db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.Table),
		Key:                       key,
		UpdateExpression:          aws.String(updateExp),
		ExpressionAttributeValues: updateMap,
		ExpressionAttributeNames:  updateNames,
		ReturnValues:              returnvals})

	if e != nil {
		var re error
		if awsErr, ok := e.(awserr.Error); ok {
			re = errors.New("Unable to update item in the database [" + awsErr.Error() + "]")
		} else {
			re = errors.New("Unable to update item in the database")
		}

		return nil, re
	}

	result := &dynamodbResult{}
	if dbResult.Attributes != nil {
		result.attributes = dbResult.Attributes
	}

	return result, nil
}

func queryPages(db DBer, r Request) (*dynamodbResult, error) {
	expValMap := make(map[string]*dynamodb.AttributeValue)
	expValName := make(map[string]*string)
	keyExp, err := buildConditionExpression(r.RequestConditions, expValMap, expValName)
	if err != nil {
		return nil, errors.New("Could not query items [" + err.Error() + "]")
	}
	filterExp, err := buildConditionExpression(r.ResultFitler, expValMap, expValName)
	if err != nil {
		return nil, errors.New("Could not query items [" + err.Error() + "]")
	}

	result := &dynamodbResult{}
	qI := &dynamodb.QueryInput{
		TableName:                 aws.String(r.Table),
		KeyConditionExpression:    aws.String(keyExp),
		ExpressionAttributeValues: expValMap,
		ExpressionAttributeNames:  expValName}
	if filterExp != "" {
		qI.FilterExpression = aws.String(filterExp)
	}
	if r.Index != "" {
		qI.IndexName = aws.String(r.Index)
	}
	start := r.PageSize * (r.Page - 1)
	count := r.PageSize
	resultsSeen := 0
	e := db.QueryPages(qI,
		func(page *dynamodb.QueryOutput, lastpage bool) bool {
			if int(*page.Count)+resultsSeen <= start {
				resultsSeen += int(*page.Count)
				return true
			}
			if count != 0 {
				for i := 0; i < int(*page.Count); i++ {
					if resultsSeen+i >= start {
						result.items = append(result.items, page.Items[i])
						count--
					}
					if count == 0 {
						break
					}
				}
			}
			resultsSeen += int(*page.Count)
			return true
		})
	// calculate the page count
	result.pageCount = resultsSeen / r.PageSize
	if resultsSeen%r.PageSize != 0 {
		result.pageCount++
	}
	if e != nil {
		var re error
		if awsErr, ok := e.(awserr.Error); ok {
			re = errors.New("Unable to query the database [" + awsErr.Error() + "]")
		} else {
			re = errors.New("Unable to query the database")
		}
		return nil, re
	}
	return result, nil
}

func query(db DBer, r Request) (*dynamodbResult, error) {
	expValMap := make(map[string]*dynamodb.AttributeValue)
	expValName := make(map[string]*string)

	keyExp, err := buildConditionExpression(r.RequestConditions, expValMap, expValName)
	if err != nil {
		return nil, errors.New("Could not query items [" + err.Error() + "]")
	}

	result := &dynamodbResult{}

	qI := &dynamodb.QueryInput{
		TableName:                 aws.String(r.Table),
		KeyConditionExpression:    aws.String(keyExp),
		ExpressionAttributeValues: expValMap,
		ExpressionAttributeNames:  expValName}

	if r.Limit > 0 {
		qI.Limit = aws.Int64(int64(r.Limit))
	}

	if len(r.LastKey) != 0 {
		lKey, err := marshalItems(r.LastKey)
		if err != nil {
			return nil, err
		}
		qI.ExclusiveStartKey = lKey
	}

	e := db.QueryPages(qI,
		func(p *dynamodb.QueryOutput, lastPage bool) bool {
			for i := 0; i < int(*p.Count); i++ {
				result.items = append(result.items, p.Items[i])
			}
			return true
		})

	if e != nil {
		var re error
		if awsErr, ok := e.(awserr.Error); ok {
			re = errors.New("Unable to query the database [" + awsErr.Error() + "]")
		} else {
			re = errors.New("Unable to query the database")
		}

		return nil, re
	}

	return result, nil
}

// reverseOp takes a successful operation and generates a request to undo it
func reverseOp(o op) *Request {
	r := &Request{}
	switch o.request.Action {
	case Put:
		r.Action = Delete
		r.Key = o.request.Key
		r.Table = o.request.Table
	case Delete:
		r.Action = Put
		r.Key = o.request.Key
		r.Table = o.request.Table
		r.Item = unmarshalItems(o.result.attributes)
	case Update:
		r.Action = Update
		r.Key = o.request.Key
		r.Table = o.request.Table
		r.Updates = make([]UpdateValue, 0)
		attrs := unmarshalItems(o.result.attributes)
		for _, v := range o.request.Updates {
			if v.Action == Put && attrs[v.Path] == nil {
				r.Updates = append(r.Updates, UpdateValue{Action: Delete})
			} else {
				if attrs[v.Path] != nil {
					r.Updates = append(r.Updates, UpdateValue{Action: Put, Path: v.Path, Value: attrs[v.Path]})
				}
			}
		}
	}

	return r
}
