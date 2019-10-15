package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gofrs/uuid"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"

	auth "github.com/fredericalix/yic_auth"
	workflow "github.com/fredericalix/yic_workflow-engine"

	_ "net/http/pprof"
)

func failOnError(err error, msg string) {
	if err != nil {
		msg := fmt.Errorf("%s: %s", msg, err)
		log.Fatal(msg)
	}
}

type handler struct {
	workerName string

	db *sql.DB

	conn      *amqp.Connection
	sendCmdCh *amqp.Channel

	workflows  map[string]*Workflow
	sensorLock sync.RWMutex
}

func main() {
	viper.AutomaticEnv()
	viper.SetDefault("PORT", "8080")
	viper.SetDefault("WORKER_NAME", "workflow-engine0")

	configFile := flag.String("config", "./config.toml", "path of the config file")
	flag.Parse()
	viper.SetConfigFile(*configFile)
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Printf("cannot read config file: %v\nUse env instead\n", err)
	}

	h := &handler{
		workerName: viper.GetString("WORKER_NAME"),
		workflows:  make(map[string]*Workflow),
	}

	h.db, err = newPostgreSQL(viper.GetString("POSTGRESQL_URI"))
	failOnError(err, "Failed to connect to PostgreSQL")
	h.conn, err = amqp.Dial(viper.GetString("RABBITMQ_URI"))
	failOnError(err, "Failed to connect to RabbitMQ")

	e := echo.New()
	e.Use(middleware.Logger())
	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Echo ping status OK\n")
	})
	e.GET("/workflow/running", h.getRunningWorkflow)
	e.GET("/workflow/running/debug", h.getRunningWorkflowDebug)
	e.GET("/workflow/running/debug/:id", h.getRunningWorkflowDebugDot)
	e.GET("/workflow/operation", getPossibleOperator)
	e.GET("/workflow/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux), middleware.Rewrite(map[string]string{"/workflow/*": "/$1"}))

	roles := auth.Roles{"ui": "rw"}
	sessions := auth.NewValidHTTP(viper.GetString("AUTH_CHECK_URI"))
	authM := auth.Middleware(sessions, roles)
	e.GET("/workflow", h.getWorkflows, authM)
	e.GET("/workflow/:wid", h.getWorkflowID, authM)
	e.GET("/workflow/history/:wid", h.getWorkflowIDHistory, authM)
	e.POST("/workflow", h.postWorkflow, authM)
	e.DELETE("/workflow/:wid", h.deleteWorkflow, authM)
	e.GET("/workflow/outputs", h.getWorkflowOutput, authM)
	e.GET("/workflow/outputs/:wid", h.getWorkflowOutputID, authM)

	go func() {
		log.Fatalf("closing: %s", <-h.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	h.sendCmdCh = openCommandChannel(h.conn, h.workerName)
	go h.handleIncomingCommand()
	go h.handleAccountDeleted()

	go h.startWorkflows()

	// start the server
	host := ":" + viper.GetString("PORT")
	tlscert := viper.GetString("TLS_CERT")
	tlskey := viper.GetString("TLS_KEY")
	if tlscert == "" || tlskey == "" {
		e.Logger.Error("No cert or key provided. Start server using HTTP instead of HTTPS !")
		e.Logger.Fatal(e.Start(host))
	}
	e.Logger.Fatal(e.StartTLS(host, tlscert, tlskey))
}

func (h *handler) startWorkflows() {
	query := `SELECT account_id, id, created_at, name, version, graph
	FROM workflow JOIN (
		SELECT id as maxid, MAX(created_at) as maxc
		FROM workflow
		GROUP BY account_id, maxid
	) w
	ON w.maxc = created_at AND w.maxid = id
	WHERE worker=$1;`
	rows, err := h.db.Query(query, h.workerName)
	if err != nil && err != sql.ErrNoRows {
		failOnError(err, "could not retreive workflow to lunch")
	}
	for rows.Next() {
		var w dbWorkflow
		lerr := rows.Scan(&w.AccountID, &w.ID, &w.CreatedAt, &w.Name, &w.Version, &w.Graph)
		if lerr != nil {
			err = lerr
			log.Printf("could not scan workflow from db: %v", err)
			continue
		}
		if err := h.startWorkflow(w); err != nil {
			log.Printf("could not lunch %v %v: %v", w.ID, w.AccountID, err)
		}
	}
}

func (h *handler) startWorkflow(w dbWorkflow) error {
	graph := workflow.FlowGraph{
		ID:   w.ID,
		AID:  w.AccountID,
		Name: w.Name,
	}
	err := json.Unmarshal(w.Graph, &graph.Flow)
	if err != nil {
		return err
	}

	wo, err := newWorkflow(h.conn, &graph, h.workerName)
	if err != nil {
		return err
	}
	log.Printf("start workflow %v %v", w.AccountID, w.ID)

	h.sensorLock.Lock()
	defer h.sensorLock.Unlock()
	h.workflows[w.ID.String()] = wo
	return nil
}

// stopWorkflow stop the given workflow, return true if the workflow was running, false otherwi
func (h *handler) stopWorkflow(wid string) bool {
	h.sensorLock.Lock()
	defer h.sensorLock.Unlock()

	w, found := h.workflows[wid]
	if !found {
		return false
	}
	delete(h.workflows, wid)
	w.Stop()
	return true
}

func (h *handler) getRunningWorkflow(c echo.Context) error {
	type res struct {
		ID   uuid.UUID `json:"id"`
		AID  uuid.UUID `json:"account_id"`
		Name string    `json:"name"`
	}
	var ws []res
	h.sensorLock.RLock()
	for _, w := range h.workflows {
		ws = append(ws, res{ID: w.graph.ID, AID: w.graph.AID, Name: w.graph.Name})
	}
	h.sensorLock.RUnlock()
	return c.JSON(http.StatusOK, ws)
}

func (h *handler) getRunningWorkflowDebug(c echo.Context) error {
	h.sensorLock.RLock()
	defer h.sensorLock.RUnlock()
	var res []*workflow.FlowGraph
	for _, w := range h.workflows {
		res = append(res, w.graph)
	}
	return c.JSON(http.StatusOK, res)
}

func (h *handler) getRunningWorkflowDebugDot(c echo.Context) error {
	id := c.Param("id")

	h.sensorLock.RLock()
	defer h.sensorLock.RUnlock()
	w, found := h.workflows[id]
	if !found {
		return c.NoContent(http.StatusNotFound)
	}

	var buf bytes.Buffer
	buf.WriteString(`<!DOCTYPE html>
<meta charset="utf-8">
<body>
<script src="//d3js.org/d3.v4.min.js"></script>
<script src="https://unpkg.com/viz.js@1.8.0/viz.js" type="javascript/worker"></script>
<script src="https://unpkg.com/d3-graphviz@1.4.0/build/d3-graphviz.min.js"></script>
<div id="graph" style="text-align: center;"></div>
<script>

d3.select("#graph").graphviz()
    .fade(false)
    .renderDot(`)
	buf.WriteString("`")
	_, err := w.graph.WriteDotFormat(&buf)
	if err != nil {
		c.HTML(http.StatusInternalServerError, err.Error())
	}
	buf.WriteString("`);\n</script>")

	return c.HTML(http.StatusOK, buf.String())
}
