package tests

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"

	"github.com/angopher/chronus/cmd/influxd/run"
	"github.com/angopher/chronus/coordinator"
	"github.com/angopher/chronus/raftmeta"
	"github.com/angopher/chronus/x"
)

func NewDefaultConfig() *Config {
	c := &Config{rootPath: "/tmp/.chronus_test_path", Config: run.NewConfig()}
	c.Meta.Dir = "./meta"
	c.Data.Dir = "./data"
	c.Data.WALDir = "./wal"

	c.HTTPD.BindAddress = "127.0.0.1:2001"
	c.BindAddress = "127.0.0.1:3001"
	return c
}

type ClusterManager struct {
	DataServerNumber int //data server number
	MetaServerNumber int //meta server number
	Config           *Config
	dataSrvs         map[uint64]*DataServer
	metaSrvs         map[uint64]*MetaServer
	Status           string
}

func NewClusterManager(c *Config, size int) *ClusterManager {
	return &ClusterManager{
		Config:           c,
		DataServerNumber: size,
		MetaServerNumber: 1,
		dataSrvs:         make(map[uint64]*DataServer),
		metaSrvs:         make(map[uint64]*MetaServer),
		Status:           EMPTY,
	}
}

func OpenCluster(c *Config, size int) *ClusterManager {
	cluster = NewClusterManager(c, size)
	err := cluster.Open()
	if err != nil {
		panic(err.Error())
	}
	return cluster
}

func (me *ClusterManager) SelectOneDataServer() *DataServer {
	return me.dataSrvs[1]
	for _, srv := range me.dataSrvs {
		if srv.Status == RUN {
			return srv
		}
	}
	return nil
}

func (me *ClusterManager) DataServerById(id uint64) *DataServer {
	return me.dataSrvs[id]
}

func (me *ClusterManager) Open() error {
	binPath := me.Config.rootPath + "/bin"

	peers := []raftmeta.Peer{}
	metaAddrs := []string{}
	for i := 0; i < me.MetaServerNumber; i++ {
		id := uint64(i + 1)
		mc := raftmeta.NewConfig()
		host, port := SplitAddr(mc.MyAddr)
		mc.MyAddr = Addr(host, port+i)
		mc.Peers = peers
		mc.RaftId = id
		rootPath := me.Config.rootPath + fmt.Sprintf("/meta_server_%d", id)
		srv := NewMetaServer(mc, id, rootPath, binPath)
		if err := srv.Open(); err != nil {
			return fmt.Errorf("open meta server failed. err:%s", err.Error())
		}

		srv.WaitRun()
		me.metaSrvs[id] = srv
		peers = append(peers, raftmeta.Peer{Addr: mc.MyAddr, RaftId: id})
		metaAddrs = append(metaAddrs, mc.MyAddr)
	}

	host, bindPortBegin := SplitAddr(me.Config.BindAddress)
	_, httpdPortBegin := SplitAddr(me.Config.HTTPD.BindAddress)
	for i := 0; i < me.DataServerNumber; i++ {
		c := new(Config)
		*c = *me.Config
		c.rootPath = fmt.Sprintf("%s/data_server_%d", me.Config.rootPath, i+1)
		c.BindAddress = Addr(host, bindPortBegin+i)
		c.HTTPD.BindAddress = Addr(host, httpdPortBegin+i)
		c.Coordinator.MetaServices = metaAddrs

		nodeID := uint64(i + 1)
		srv := NewDataServer(me, c, nodeID, binPath)
		if err := srv.Open(); err != nil {
			return err
		}
		me.dataSrvs[nodeID] = srv
	}

	me.WaitDataServersRun()
	me.Status = RUN
	return nil
}

func (me *ClusterManager) WaitDataServersRun() {
	for _, srv := range me.dataSrvs {
		srv.WaitRun()
	}
}

func (me *ClusterManager) Close() error {
	for _, d := range me.dataSrvs {
		d.Close()
	}
	for _, m := range me.metaSrvs {
		m.Close()
	}
	me.Status = STOP
	return nil
}

func (me *ClusterManager) Clean() error {
	for _, d := range me.dataSrvs {
		d.Clean()
	}
	for _, m := range me.metaSrvs {
		m.Clean()
	}
	return nil
}

type MetaServer struct {
	Config   raftmeta.Config
	BinPath  string
	RootPath string
	ID       uint64
	cmd      *exec.Cmd
	Status   string
}

func NewMetaServer(c raftmeta.Config, id uint64, rootPath, binPath string) *MetaServer {
	return &MetaServer{Config: c, ID: id, Status: EMPTY, RootPath: rootPath, BinPath: binPath}
}

func (me *MetaServer) WaitRun() {
	cli := &coordinator.MetaClientImpl{Addrs: []string{me.Config.MyAddr}}
	var err error
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		_, err = cli.Data()
		if err == nil {
			break
		}
	}
	x.Check(err)
	time.Sleep(100 * time.Millisecond)
}

func (me *MetaServer) Restart() {
	me.Close()
	x.Check(me.Open())
	me.WaitRun()
}

func (me *MetaServer) Open() error {
	root := me.RootPath
	if root == "" || root == "/" {
		return fmt.Errorf("invalid rootPath:%s", root)
	}

	fmt.Println("root:", root)
	if me.Status == EMPTY {
		os.RemoveAll(root)
		os.MkdirAll(root, os.ModePerm)
		os.MkdirAll(root+"/meta_data", os.ModePerm)

		_, err := exec.Command("cp", "-r", me.BinPath, root).Output()
		if err != nil {
			return err
		}
	}

	confPath := root + "/conf"
	os.MkdirAll(confPath, os.ModePerm)
	DumpConfig(&me.Config, confPath+"/metad.conf")

	cmd := exec.Command("bin/metad", "-config", "conf/metad.conf")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = root

	var err error
	stdoutFile := root + "/stdout"
	cmd.Stdout, err = os.OpenFile(stdoutFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}
	stderrFile := root + "/stderr"
	cmd.Stderr, err = os.OpenFile(stderrFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	me.cmd = cmd
	return nil
}

func (me *MetaServer) Close() {
	me.Status = STOP
	if me.cmd != nil {
		syscall.Kill(-me.cmd.Process.Pid, syscall.SIGKILL)
		me.cmd = nil
	}
	return
}

func (me *MetaServer) Clean() error {
	me.Status = EMPTY
	os.RemoveAll(me.RootPath)
	return nil
}

const (
	EMPTY = "empty"
	RUN   = "run"
	STOP  = "stop"
)

type DataServer struct {
	Config  *Config
	BinPath string
	Status  string
	ID      uint64
	cmd     *exec.Cmd
	cm      *ClusterManager
}

func NewDataServer(cm *ClusterManager, c *Config, NodeID uint64, binPath string) *DataServer {
	cc := &run.Config{}
	*cc = *c.Config
	config := &Config{
		rootPath: c.rootPath,
		Config:   cc,
	}
	return &DataServer{cm: cm, Config: config, Status: EMPTY, ID: NodeID, BinPath: binPath}
}

func (me *DataServer) NodeID() uint64 {
	return me.ID
}

func (me *DataServer) URL() string {
	return "http://" + me.Config.HTTPD.BindAddress
}

func (me *DataServer) WaitRun() {
	var err error
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		var resp *http.Response
		resp, err = http.Get(me.URL() + "/ping")
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
			return
		}
	}

	x.Check(err)
}

func (me *DataServer) Restart() {
	me.Close()
	x.Check(me.Open())
	me.WaitRun()
}

func (me *DataServer) Clean() {
	me.Status = EMPTY
	os.RemoveAll(me.Config.rootPath)
}

func (me *DataServer) Open() error {
	fmt.Printf("----------------open data server. url=%s\n", me.URL())
	root := me.Config.rootPath
	if root == "" || root == "/" {
		return fmt.Errorf("invalid rootPath:%s", root)
	}

	if me.Status == EMPTY {
		os.RemoveAll(root)
		os.MkdirAll(root, os.ModePerm)

		_, err := exec.Command("cp", "-r", me.BinPath, root).Output()
		if err != nil {
			return err
		}

	}

	confPath := root + "/conf"
	os.MkdirAll(confPath, os.ModePerm)
	DumpConfig(me.Config, confPath+"/influxd.conf")

	cmd := exec.Command("bin/influxd", "-config", "conf/influxd.conf")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = root

	var err error
	stdoutFile := root + "/stdout"
	cmd.Stdout, err = os.OpenFile(stdoutFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}
	stderrFile := root + "/stderr"
	cmd.Stderr, err = os.OpenFile(stderrFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}
	me.cmd = cmd
	me.Status = RUN
	return nil
}

func (me *DataServer) SetLogOutput(w io.Writer) {
}

func (me *DataServer) close() {
	me.cm.Close()
}

func (me *DataServer) Close() {
	me.Status = STOP
	if me.cmd != nil {
		syscall.Kill(-me.cmd.Process.Pid, syscall.SIGKILL)
		me.cmd = nil
	}
}

func (me *DataServer) Closed() bool {
	return me.Status == STOP
}

func (me *DataServer) CreateDatabase(db string) (*meta.DatabaseInfo, error) {
	stmt := fmt.Sprintf("CREATE+DATABASE+%s", db)

	_, err := HTTPPost(me.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return nil, err
	}
	time.Sleep(100 * time.Millisecond)
	return &meta.DatabaseInfo{}, nil
}

func (me *DataServer) CreateDatabaseAndRetentionPolicy(db string, rp *meta.RetentionPolicySpec, makeDefault bool) error {
	if _, err := me.CreateDatabase(db); err != nil {
		return err
	}

	stmt := fmt.Sprintf("CREATE+RETENTION+POLICY+%s+ON+\"%s\"+DURATION+%s+REPLICATION+%v+SHARD+DURATION+%s",
		rp.Name, db, rp.Duration, *rp.ReplicaN, rp.ShardGroupDuration)
	if makeDefault {
		stmt += "+DEFAULT"
	}

	_, err := HTTPPost(me.URL()+"/query?q="+stmt, nil)
	time.Sleep(100 * time.Millisecond)
	return err
}

func (me *DataServer) DropDatabase(db string) error {
	stmt := fmt.Sprintf("DROP+DATABASE+%s", db)

	_, err := HTTPPost(me.URL()+"/query?q="+stmt, nil)
	time.Sleep(100 * time.Millisecond)
	return err
}

func (me *DataServer) Reset() error {
	if me.Status != RUN {
		return nil
	}
	stmt := fmt.Sprintf("SHOW+DATABASES")
	results, err := HTTPPost(me.URL()+"/query?q="+stmt, nil)
	if err != nil {
		return err
	}

	resp := &httpd.Response{}
	if resp.UnmarshalJSON([]byte(results)); err != nil {
		return err
	}

	for _, db := range resp.Results[0].Series[0].Values {
		x.Check(me.DropDatabase(fmt.Sprintf("%s", db[0])))
	}
	return nil
}

func (me *DataServer) Query(query string) (string, error) {
	return me.QueryWithParams(query, nil)
}

func (me *DataServer) QueryWithParams(query string, values url.Values) (string, error) {
	var v url.Values
	if values == nil {
		v = url.Values{}
	} else {
		v, _ = url.ParseQuery(values.Encode())
	}
	v.Set("q", query)
	return HTTPPost(me.URL()+"/query?"+v.Encode(), nil)
}

func (me *DataServer) Write(db, rp, body string, params url.Values) (string, error) {
	if params == nil {
		params = url.Values{}
	}
	if params.Get("db") == "" {
		params.Set("db", db)
	}
	if params.Get("rp") == "" {
		params.Set("rp", rp)
	}
	resp, err := http.Post(me.URL()+"/write?"+params.Encode(), "", strings.NewReader(body))
	if err != nil {
		return "", err
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return "", WriteError{statusCode: resp.StatusCode, body: string(MustReadAll(resp.Body))}
	}
	return string(MustReadAll(resp.Body)), nil
}

func (me *DataServer) MustWrite(db, rp, body string, params url.Values) string {
	results, err := me.Write(db, rp, body, params)
	if err != nil {
		panic(err)
	}
	return results
}

func (me *DataServer) WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error {
	level := "all"
	switch consistencyLevel {
	case models.ConsistencyLevelAny:
		level = "any"
	case models.ConsistencyLevelOne:
		level = "one"
	case models.ConsistencyLevelQuorum:
		level = "quorum"
	case models.ConsistencyLevelAll:
		level = "all"
	default:
		return errors.New("invalid consistency level")
	}
	params := url.Values{
		"db":          []string{database},
		"rp":          []string{retentionPolicy},
		"consistency": []string{level},
	}

	for _, p := range points {
		_, err := me.Write(database, retentionPolicy, p.String(), params)
		if err != nil {
			return err
		}
	}
	return nil
}

func (me *DataServer) CreateSubscription(database, rp, name, mode string, destinations []string) error {
	dests := make([]string, 0, len(destinations))
	for _, d := range destinations {
		dests = append(dests, "'"+d+"'")
	}

	stmt := fmt.Sprintf("CREATE+SUBSCRIPTION+%s+ON+\"%s\".\"%s\"+DESTINATIONS+%v+%s",
		name, database, rp, mode, strings.Join(dests, ","))

	_, err := HTTPPost(me.URL()+"/query?q="+stmt, nil)
	return err
}

func HTTPPost(url string, content []byte) (results string, err error) {
	buf := bytes.NewBuffer(content)
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return "", err
	}
	body := strings.TrimSpace(string(MustReadAll(resp.Body)))
	switch resp.StatusCode {
	case http.StatusBadRequest:
		if !expectPattern(".*error parsing query*.", body) {
			return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
		}
		return body, nil
	case http.StatusOK, http.StatusNoContent:
		return body, nil
	default:
		return "", fmt.Errorf("unexpected status code: code=%d, body=%s", resp.StatusCode, body)
	}
}

func SplitAddr(addr string) (host string, port int) {
	s := strings.Split(addr, ":")
	if len(s) != 2 {
		panic("bad addr=" + addr)
	}

	port, err := strconv.Atoi(s[1])
	if err != nil {
		panic(err.Error())
	}
	return s[0], port
}

func Addr(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func DumpConfig(c interface{}, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()
	return toml.NewEncoder(w).Encode(c)
}
