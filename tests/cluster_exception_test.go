package tests

import (
	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/toml"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestServer_Replication_Normal(t *testing.T) {
	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	//t.Parallel()
	CleanCluster()
	c := NewConfig()
	cm := OpenCluster(c, 6)
	defer cm.Clean()
	defer cm.Close()

	s := cm.SelectOneDataServer()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 2, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.writes = make(Writes, 0, 10)
	for j := 0; j < cap(test.writes); j++ {
		writes := make([]string, 0, 10000)
		for i := 0; i < cap(writes); i++ {
			writes = append(writes, fmt.Sprintf(`cpu,t=%d v=%d %d`, i, j*cap(writes)+i, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()+int64(j*cap(writes)+i)))
		}
		test.writes = append(test.writes, &Write{data: strings.Join(writes, "\n")})
	}

	// These queries use index sketches to estimate cardinality.
	test.addQueries([]*Query{
		&Query{
			name:    `select all count`,
			command: "select count(*) from cpu",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v"],"values":[["1970-01-01T00:00:00Z",100000]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `select all count`,
			command: "select count(*) from cpu",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v"],"values":[["1970-01-01T00:00:00Z",100000]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Replication_Write_HintedHandoff(t *testing.T) {
	if testing.Short() || os.Getenv("GORACE") != "" || os.Getenv("APPVEYOR") != "" {
		t.Skip("Skipping test in short, race and appveyor mode.")
	}

	CleanCluster()
	c := NewConfig()
	c.HintedHandoff.Enabled = true
	c.HintedHandoff.RetryInterval = toml.Duration(time.Second)
	c.HintedHandoff.RetryMaxInterval = toml.Duration(time.Second)
	c.HintedHandoff.Dir = "./hinted_handoff"
	cm := OpenCluster(c, 3)
	defer cm.Clean()
	defer cm.Close()

	first := cm.DataServerById(1)
	//TODO:defer cm.Close()

	fmt.Println("first server url:", first.URL())
	if err := first.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 3, 0), true); err != nil {
		t.Fatal(err)
	}

	first.Close()
	//wait server killed
	time.Sleep(time.Second)

	second := cm.DataServerById(2)
	fmt.Println("second server url:", second.URL())
	points, _ := models.ParsePointsString(fmt.Sprintf(`cpu,t=%d v=%d %d`, 1, 1, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()))
	err := second.WritePoints("db0", "rp0", models.ConsistencyLevelAll, nil, points)
	if err == nil {
		t.Error("expect err != nil. actual err is nil")
	}

	query := &Query{
		name:    `select all count`,
		command: "select count(*) from cpu",
		exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		params:  url.Values{"db": []string{"db0"}},
	}

	if err := query.Execute(second); err != nil {
		t.Error(query.Error(err))
	} else if !query.success() {
		t.Error(query.failureMessage())
	}

	points, _ = models.ParsePointsString(fmt.Sprintf(`cpu,t=%d v=%d %d`, 2, 1, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()))
	err = second.WritePoints("db0", "rp0", models.ConsistencyLevelAny, nil, points)
	if err != nil {
		t.Errorf("unexpected err:%v", err)
	}

	query = &Query{
		name:    `select all count`,
		command: "select count(*) from cpu",
		exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
		params:  url.Values{"db": []string{"db0"}},
	}

	if err := query.Execute(second); err != nil {
		t.Error(query.Error(err))
	} else if !query.success() {
		t.Error(query.failureMessage())
	}

	points, _ = models.ParsePointsString(fmt.Sprintf(`cpu,t=%d v=%d %d`, 3, 1, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()))
	err = second.WritePoints("db0", "rp0", models.ConsistencyLevelQuorum, nil, points)
	if err != nil {
		t.Errorf("unexpected err:%v", err)
	}

	second.Close()
	time.Sleep(time.Second)

	third := cm.DataServerById(3)

	query = &Query{
		name:    `select all count`,
		command: "select count(*) from cpu",
		exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v"],"values":[["1970-01-01T00:00:00Z",3]]}]}]}`,
		params:  url.Values{"db": []string{"db0"}},
	}

	if err := query.Execute(third); err != nil {
		t.Error(query.Error(err))
	} else if !query.success() {
		t.Error(query.failureMessage())
	}

	points, _ = models.ParsePointsString(fmt.Sprintf(`cpu,t=%d v=%d %d`, 4, 1, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()))
	err = third.WritePoints("db0", "rp0", models.ConsistencyLevelQuorum, nil, points)
	if err == nil {
		t.Errorf("expect err != nil. actual err is nil")
	}

	first.Open()
	second.Open()
	time.Sleep(5 * time.Second) //wait data sync done

	query = &Query{
		name:    `select all count`,
		command: "select count(*) from cpu",
		exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
		params:  url.Values{"db": []string{"db0"}},
	}

	if err := query.Execute(first); err != nil {
		t.Error(query.Error(err))
	} else if !query.success() {
		t.Error(query.failureMessage())
	}
}

func TestServer_Replication_Write_NoHintedHandoff(t *testing.T) {
	CleanCluster()
	c := NewConfig()
	c.HintedHandoff.Enabled = false
	cm := OpenCluster(c, 2)
	defer cm.Clean()
	defer cm.Close()

	first := cm.DataServerById(1)
	//TODO:defer cm.Close()

	fmt.Println("first server url:", first.URL())
	if err := first.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 2, 0), true); err != nil {
		t.Fatal(err)
	}

	first.Close()
	//wait server killed
	time.Sleep(time.Second)

	second := cm.DataServerById(2)
	fmt.Println("second server url:", second.URL())
	points, _ := models.ParsePointsString(fmt.Sprintf(`cpu,t=%d v=%d %d`, 1, 1, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:01Z").UnixNano()))
	err := second.WritePoints("db0", "rp0", models.ConsistencyLevelAll, nil, points)
	if err == nil {
		t.Error("expect err != nil. actual err is nil")
	}

	query := &Query{
		name:    `select all count`,
		command: "select count(*) from cpu",
		exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		params:  url.Values{"db": []string{"db0"}},
	}

	if err := query.Execute(second); err != nil {
		t.Error(query.Error(err))
	} else if !query.success() {
		t.Error(query.failureMessage())
	}

	first.Open()
	time.Sleep(5 * time.Second)

	query = &Query{
		name:    `select all count`,
		command: "select count(*) from cpu",
		exp:     `{"results":[{"statement_id":0}]}`,
		params:  url.Values{"db": []string{"db0"}},
	}

	if err := query.Execute(first); err != nil {
		t.Error(query.Error(err))
	} else if !query.success() {
		t.Error(query.failureMessage())
	}
}
