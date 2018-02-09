package store

type Action int32
type Condition int32
type Relationship int32

// Actions
const (
	Put Action = iota
	Get
	Update
	Delete
	Query
	QueryPager
)

// Conditions
const (
	Exist Condition = iota
	NotExist
	GreaterThan
	LessThan
	Equal
	BeginsWith
)

// Relsationships
const (
	And Relationship = iota
	Or
)

// Request is a generic way to represent database requests
type Request struct {
	Table             string
	Action            Action
	Key               map[string]interface{} // The index for the item
	Item              map[string]interface{} // For PUT operations the data to insert into the database
	Updates           []UpdateValue          // For Update operations, the updates to perform
	PageSize          int                    // size of the pages to return
	Page              int                    // For query, limit the results to this number
	Index             string                 // the index to use
	ReturnValues      string
	ConsistentRead    bool
	LiveData          bool
	Limit             int
	LastKey           map[string]interface{} // For queries that were limited, the last evaluated key
	RequestConditions []RequestCondition
	ResultFitler      []RequestCondition
}

// RequestCondition specifies conditions that must be true for the request to take place
type RequestCondition struct {
	Field        string
	Type         Condition
	Relationship Relationship
	Value        interface{}
}

func (r RequestCondition) RelationshipString() string {
	switch r.Relationship {
	case And:
		return "AND"
	case Or:
		return "OR"
	}

	return ""
}

// UpdateValue represents an update to a database attribtes
type UpdateValue struct {
	Action Action
	Path   string
	Value  interface{}
}

// AddKey adds a RequestItem to the Key and returns the request
func (r *Request) AddKey(name string, value interface{}) *Request {
	if r.Key == nil {
		r.Key = make(map[string]interface{})
	}
	r.Key[name] = value
	return r
}

// AddItem adds a RequestItem to the item slice
func (r *Request) AddItem(name string, value interface{}) *Request {
	if r.Item == nil {
		r.Item = make(map[string]interface{})
	}
	r.Item[name] = value
	return r
}

// AddCondition adds a request condition
func (r *Request) AddCondition(field string, condition Condition, relationship Relationship, value interface{}) *Request {
	r.RequestConditions = append(r.RequestConditions, RequestCondition{field, condition, relationship, value})
	return r
}

// And adds a request "and" condition
func (r *Request) And(field string, condition Condition, value interface{}) *Request {
	r.RequestConditions = append(r.RequestConditions, RequestCondition{field, condition, And, value})
	return r
}

// And adds a request "or" condition
func (r *Request) Or(field string, condition Condition, value interface{}) *Request {
	r.RequestConditions = append(r.RequestConditions, RequestCondition{field, condition, Or, value})
	return r
}

func (r *Request) AddUpdateValue(path string, action Action, value interface{}) *Request {

	if r.Updates == nil {
		r.Updates = make([]UpdateValue, 0)
	}

	if string(path[0]) != "/" {
		path = "/" + path
	}

	r.Updates = append(r.Updates, UpdateValue{Action: action, Path: path, Value: value})

	return r
}

func (r *Request) UpdateAddRemoveValue(fieldName string, len int, value interface{}) {
	if len == 0 {
		r.AddUpdateValue("/"+fieldName, Delete, nil)
	} else {
		r.AddUpdateValue("/"+fieldName, Update, value)
	}
}
