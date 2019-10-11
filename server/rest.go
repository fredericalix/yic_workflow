package main

import (
	"net/http"
	"time"
	"encoding/json"
	"github.com/gofrs/uuid"
	"github.com/labstack/echo"

	"github.com/fredericalix/yic_auth"
	workflow "github.com/fredericalix/yic_workflow-engine"
)

// swagger:model
type swaggerWorkflow struct {
	//swagger:strfmt uuid
	//example: 671dd0e5-895c-425e-9303-8c14cc3fc46c
	ID string `json:"id"`
	//swagger:strfmt uuid
	// example: fe22559b-ba9f-404f-9732-f89e830969f2
	AccoundID string    `json:"accound_id"`
	CreatedAt time.Time `json:"created_at"`

	// The worker name to assign the workflow
	// example: worklow-engine0
	Worker  string `json:"worker"`
	Name    string `json:"name"`
	Version string `json:"version"`
	Graph   map[string]struct {
		// UUID of the sensors of the input or output operator
		ID string `json:"id,omitempty"`
		// Name of the json field for input or ouput operator
		Name string `json:"name,omitempty"`
		// Return type
		Type string `json:"type"`
		// Operator of the computation
		Operator string `json:"operator"`
		// List of input key ("property1", "property2")
		Inputs    []string `json:"inputs,omitempty"`
		Condition []string `json:"condition,omitempty"`

		// Precomputed vaue for constant
		ComputedValue string `json:"computed_value,omitempty"`
	} `json:"graph"`
}

// Successfull
// swagger:response workflowsResponse
type workflowsResponse struct {
	// in: body
	// required: true
	Body []swaggerWorkflow
}

// Successfull
// swagger:response workflowResponse
type workflowResponse struct {
	// in: body
	// required: true
	Body swaggerWorkflow
}

// swagger:route GET /workflow Workflow workflows
//
// Workflows
//
// Get every worflow associate with the account
//
//     Produces:
//     - application/json
//
//     Schemes: http, https
//
//     Responses:
//       200: workflowsResponse
//       400:
//       500:

func (h *handler) getWorkflows(c echo.Context) error {
	// Auth
	account := c.Get("account").(auth.Account)

	// SQL query
	wf := make([]dbWorkflow, 0, 16)
	query := `SELECT account_id, id, created_at, name, worker, version
	FROM workflow JOIN (
		SELECT id as maxid, MAX(created_at) as maxc
		FROM workflow
		WHERE account_id = $1
		GROUP BY maxid
	) w
	ON w.maxc = created_at AND w.maxid = id AND account_id = $1
	;`
	rows, err := h.db.Query(query, account.ID)
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v: %v", account.ID, err)
		return c.NoContent(http.StatusInternalServerError)
	}
	for rows.Next() {
		var w dbWorkflow
		lerr := rows.Scan(&w.AccountID, &w.ID, &w.CreatedAt, &w.Name, &w.Worker, &w.Version, &w.Graph)
		if lerr != nil {
			err = lerr
			continue
		}
		wf = append(wf, w)
	}
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v: %v", account.ID, err)
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.JSON(http.StatusOK, wf)
}

//swagger:parameters id workflow workflowHistory workflowOuputID delworkflowID
type idParam struct {
	//in:path
	//required:true
	//example:671dd0e5-895c-425e-9303-8c14cc3fc46c
	ID string `json:"id"`
}

// swagger:route GET /workflow/{id} Workflow workflow
//
// Workflow
//
// Get a workflow by its id
//
//     Produces:
//     - application/json
//
//     Schemes: http, https
//
//     Responses:
//       200: workflowResponse
//       400:
//       500:
func (h *handler) getWorkflowID(c echo.Context) error {
	// Auth
	account := c.Get("account").(auth.Account)
	wid := c.Param("wid")

	query := `SELECT account_id, id, created_at, name, worker, version, graph
		FROM workflow
		WHERE created_at = (
			SELECT MAX(created_at) FROM workflow
			WHERE account_id = $1 AND id = $2
		)
		AND account_id = $1 AND id = $2
	;`
	row := h.db.QueryRow(query, account.ID, wid)
	if row == nil {
		c.Logger().Errorf("cannot get workflow %v %v", account.ID, wid)
		return c.NoContent(http.StatusInternalServerError)
	}
	var w dbWorkflow
	err := row.Scan(&w.AccountID, &w.ID, &w.CreatedAt, &w.Name, &w.Worker, &w.Version, &w.Graph)
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v.%v: %v", account.ID, wid, err)
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.JSON(http.StatusOK, w)
}

// swagger:route GET /workflow/history/{id} Workflow workflowHistory
//
// Workflow History
//
// Get worflow history from a given workflow id
//
//     Produces:
//     - application/json
//
//     Schemes: http, https
//
//     Responses:
//       200: workflowsResponse
//       400:
//       500:
func (h *handler) getWorkflowIDHistory(c echo.Context) error {
	// Auth
	account := c.Get("account").(auth.Account)
	wid := c.Param("wid")

	query := `SELECT account_id, id, created_at, name, worker, version, graph
		FROM workflow
		AND account_id = $1 AND id = $2
		ORDER BY created_at
	;`
	rows, err := h.db.Query(query, account.ID, wid)
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v %v: %v", account.ID, wid, err)
		return c.NoContent(http.StatusInternalServerError)
	}
	wf := make([]dbWorkflow, 0, 16)
	for rows.Next() {
		var w dbWorkflow
		lerr := rows.Scan(&w.AccountID, &w.ID, &w.CreatedAt, &w.Name, &w.Worker, &w.Version)
		if lerr != nil {
			err = lerr
			continue
		}
		wf = append(wf, w)
	}
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v %v: %v", account.ID, wid, err)
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.JSON(http.StatusOK, wf)
}

type workflowOutput struct {
	WID      string   `json:"workflow_id,omitempty"`
	ID       string   `json:"id,omitempty"`
	Name     string   `json:"name,omitempty"`
	Type     string   `json:"type,omitempty"`
	Operator string   `json:"operator,omitempty"`
	Values   []string `json:"values,omitempty"`
}

// swagger:response worflowOutputResponse
type worflowOutputResponse struct {
	// in: body
	Body []workflowOutput
}

// swagger:route GET /workflow/output Workflow workflowOuput
//
// All Ouputs
//
// Get every output of the workflow's account
//
//     Produces:
//     - application/json
//
//     Schemes: http, https
//
//     Responses:
//       200: worflowOutputResponse
//       400:
//       500:
func (h *handler) getWorkflowOutput(c echo.Context) error {
	// Auth
	account := c.Get("account").(auth.Account)

	// SQL query
	query := `SELECT id, graph
	FROM workflow JOIN (
		SELECT id as maxid, MAX(created_at) as maxc
		FROM workflow
		WHERE account_id = $1
		GROUP BY maxid
	) w
	ON w.maxc = created_at AND w.maxid = id AND account_id = $1
	;`
	rows, err := h.db.Query(query, account.ID)
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v: %v", account.ID, err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var outs []workflowOutput
	for rows.Next() {
		var wid uuid.UUID
		var o json.RawMessage
		lerr := rows.Scan(&wid, &o)
		if lerr != nil {
			err = lerr
			continue
		}
		var out map[string]workflowOutput
		json.Unmarshal(o, &out)

		for _, o := range out {
			if o.Operator == "output" {
				o.WID = wid.String()
				outs = append(outs, o)
			}
		}
	}
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v: %v", account.ID, err)
		return c.NoContent(http.StatusInternalServerError)
	}
	return c.JSON(http.StatusOK, outs)
}

// swagger:response workflowOuputID
type worflowOutputIDResponse struct {
	// in: body
	Body workflowOutput
}

// swagger:route GET /workflow/output/{id} Workflow workflowOuputID
//
// Ouput ID
//
// Get every output of the given workflow
//
//     Produces:
//     - application/json
//
//     Schemes: http, https
//
//     Responses:
//       200: worflowOutputResponse
//       400:
//       500:
func (h *handler) getWorkflowOutputID(c echo.Context) error {
	// Auth
	account := c.Get("account").(auth.Account)
	wid := c.Param("wid")

	query := `SELECT graph
		FROM workflow
		WHERE created_at = (
			SELECT MAX(created_at) FROM workflow
			WHERE account_id = $1 AND id = $2
		)
		AND account_id = $1 AND id = $2
	;`
	row := h.db.QueryRow(query, account.ID, wid)
	if row == nil {
		c.Logger().Errorf("cannot get workflow %v %v", account.ID, wid)
		return c.NoContent(http.StatusInternalServerError)
	}

	var o json.RawMessage
	err := row.Scan(&o)
	if err != nil {
		c.Logger().Errorf("cannot find workflow for %v.%v: %v", account.ID, wid, err)
		return c.NoContent(http.StatusInternalServerError)
	}

	var out map[string]workflowOutput
	json.Unmarshal(o, &out)

	var outs []workflowOutput
	for _, o := range out {
		if o.Operator == "output" {
			o.WID = wid
			outs = append(outs, o)
		}
	}

	return c.JSON(http.StatusOK, outs)
}

//swagger:parameters swaggerWorkflowPost posrworkflow
type swaggerWorkflowPost struct {
	//in:body
	//required:true
	Body swaggerWorkflow
}

// swagger:route POST /workflow Workflow posrworkflow
//
// Workflow
//
// Create a new Workflow.
//
// Consumes:
// - application/json
// Produces:
// - application/json
// Schemes: http, https
// Responses:
//   200: workflowResponse
//   400:
//   500:
func (h *handler) postWorkflow(c echo.Context) error {
	// Auth
	account := c.Get("account").(auth.Account)

	// Validate json inpu
	var w dbWorkflow
	if err := c.Bind(&w); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	}

	w.CreatedAt = time.Now()
	w.AccountID = account.ID
	if w.ID == uuid.Nil {
		w.ID = uuid.Must(uuid.NewV4())
	}
	if w.Worker == "" {
		w.Worker = h.workerName
	}

	// Check the validity of the graph
	graph := workflow.FlowGraph{
		ID:   w.ID,
		AID:  w.AccountID,
		Name: w.Name,
	}
	err := json.Unmarshal(w.Graph, &graph.Flow)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	}
	if err := graph.Build(); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	}

	// Insert into DB
	_, err = h.db.Exec("INSERT INTO workflow (account_id, id, created_at, worker, name, version, graph) VALUES ($1,$2,$3,$4,$5,$6,$7);",
		w.AccountID,
		w.ID,
		w.CreatedAt,
		w.Worker,
		w.Name,
		w.Version,
		w.Graph,
	)
	if err != nil {
		c.Logger().Errorf("cannot insert workflow for %v: %v", account.ID, err)
		return c.NoContent(http.StatusInternalServerError)
	}

	h.sendStarCommand(w)

	return c.JSON(http.StatusOK, w)
}

// swagger:route DELETE /workflow/output/{id} Workflow delworkflowID
//
// Workflow
//
// Delete the given workflow
//
//     Produces:
//     - application/json
//
//     Schemes: http, https
//
//     Responses:
//       200:
//       400:
//       500:
func (h *handler) deleteWorkflow(c echo.Context) error {
	// Auth
	account := c.Get("account").(auth.Account)
	wid := c.Param("wid")

	_, err := h.db.Exec("DELETE FROM workflow WHERE id=$1 and account_id=$2;", wid, account.ID)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	h.sendStopCommand(account.ID.String(), wid)

	// h.flows.Delete(wid)

	return c.NoContent(http.StatusOK)
}

func getPossibleOperator(c echo.Context) error {
	return c.JSON(http.StatusOK, workflow.Operations)
}
