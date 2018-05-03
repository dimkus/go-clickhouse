package clickhouse

import (
	"errors"
	"strings"
	"net/url"
	"encoding/json"
)

type External struct {
	Name      string
	Structure string
	Data      []byte
}

type Func struct {
	Name string
	Args interface{}
}

type Query struct {
	Stmt      string
	args      []interface{}
	externals []External
	params url.Values
}

type Stat struct {
	Rows     				int	`json:"rows"`
	RowsBeforeLimitAtLeast	int	`json:"rows_before_limit_at_least"`
	Stat struct {
		Elapsed     float64	`json:"elapsed"`
		RowsRead 	int     `json:"rows_read"`
		BytesRead 	int     `json:"bytes_read"`
	} `json:"statistics"`
}

// Adding external dictionary
func (q *Query) AddExternal(name string, structure string, data []byte) {
	q.externals = append(q.externals, External{Name: name, Structure: structure, Data: data})
}

// Additional parameters like: max_memory_usage, etc.
func (q Query) AddParam(name string, value string) {
	q.params.Add(name, value)
}

func (q Query) MergeParams(params url.Values) {
	for key, value := range params {
		if q.params.Get(key) == "" {
			q.params.Set(key, value[0])
		}
	}
}

func (q Query) Iter(conn *Conn) *Iter {
	if conn == nil {
		return &Iter{err: errors.New("Connection pointer is nil")}
	}
	resp, err := conn.transport.Exec(conn, q, false)
	if err != nil {
		return &Iter{err: err}
	}

	err = errorFromResponse(resp)
	if err != nil {
		return &Iter{err: err}
	}

	return &Iter{text: resp}
}

func (r *Iter) Len() int {
	return len(r.text)
}

func (q Query) Exec(conn *Conn) (err error) {
	_, err = q.fetch(conn)
	return err
}

func (q Query) fetch(conn *Conn) (resp string, err error) {
	if conn == nil {
		return "", errors.New("Connection pointer is nil")
	}
	resp, err = conn.transport.Exec(conn, q, false)
	if err == nil {
		err = errorFromResponse(resp)
	}

	return resp, err
}

func (q Query) readData(resp []byte, obj interface{}) (err error) {
	var readObj struct{
		Data json.RawMessage `json:"data"`
	}

	errUnmarshal := json.Unmarshal([]byte(resp), &readObj)
	if errUnmarshal != nil {
		return errUnmarshal
	}

	errUnmarshalObj := json.Unmarshal(readObj.Data, &obj)
	if errUnmarshalObj != nil {
		return errUnmarshalObj
	}
	return nil
}

func (q Query) ExecScan(conn *Conn, dataObj interface{}) (error) {
	q.Stmt += " FORMAT JSON"
	resp, err := q.fetch(conn)

	if err == nil {
		err := q.readData([]byte(resp), &dataObj)

		if err != nil {
			return err
		}
	}

	return err
}

func (q Query) ExecScanStat(conn *Conn, dataObj interface{}) (Stat, error) {
	q.Stmt += " FORMAT JSON"
	resp, err := q.fetch(conn)

	statObj := Stat{}
	if err == nil {
		err = q.readData([]byte(resp), &dataObj)

		if err != nil {
			return statObj, err
		}

		errUnmarshal := json.Unmarshal([]byte(resp), &statObj)
		if errUnmarshal != nil {
			return statObj, errUnmarshal
		}
	}

	return statObj, err
}

type Iter struct {
	err  error
	text string
}

func (r *Iter) Error() error {
	return r.err
}

func (r *Iter) Scan(vars ...interface{}) bool {
	row := r.fetchNext()
	if len(row) == 0 {
		return false
	}
	a := strings.Split(row, "\t")
	if len(a) < len(vars) {
		return false
	}
	for i, v := range vars {
		err := unmarshal(v, a[i])
		if err != nil {
			r.err = err
			return false
		}
	}
	return true
}

func (r *Iter) fetchNext() string {
	var res string
	pos := strings.Index(r.text, "\n")
	if pos == -1 {
		res = r.text
		r.text = ""
	} else {
		res = r.text[:pos]
		r.text = r.text[pos+1:]
	}
	return res
}
