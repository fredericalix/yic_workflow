package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	workflow "github.com/fredericalix/yic_workflow-engine"
	"github.com/gofrs/uuid"
	"github.com/streadway/amqp"
)

func randName(l int) []byte {
	const ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, l)
	for i := range b {
		b[i] = ALPHABET[rand.Intn(len(ALPHABET))]
	}
	return b
}

// Workflow ampq handler
type Workflow struct {
	graph  *workflow.FlowGraph
	ch     *amqp.Channel
	sch    *amqp.Channel
	closed chan struct{}
}

// Stop the workflow
func (w Workflow) Stop() {
	w.ch.Close()
	w.sch.Close()
	w.closed <- struct{}{}
}

func newWorkflow(conn *amqp.Connection, w *workflow.FlowGraph, workerName string) (*Workflow, error) {
	var err error
	wo := &Workflow{
		graph:  w,
		closed: make(chan struct{}),
	}
	err = wo.graph.Build()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	oldSensors, err := findLatestSensors(ctx, conn, w.AID, "")
	if err != nil {
		log.Printf("could not find latest sensors messages: %v\n", err)
	}

	ch, err := conn.Channel()
	wo.ch = ch
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		"sensors", // name
		"topic",   // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	qname := fmt.Sprintf("%s_%s_%s_%s", workerName, w.AID, w.ID, randName(6))
	q, err := ch.QueueDeclare(
		qname, // name
		false, // durable
		true,  // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,     // queue
		workerName, // consumer
		true,       // auto ack
		false,      // exclusive
		false,      // no local
		false,      // no wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")

	// Only bind input sensor to the queue
	for sensorID := range w.Inputs {
		key := fmt.Sprintf("%s.%s", w.AID, sensorID)
		err := ch.QueueBind(
			q.Name,    // queue name
			key,       // routing key
			"sensors", // exchange
			false,     // no wait
			nil,       // args
		)
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("For %s bind to %s\n", w.ID, key)
	}

	senbSensor, err := conn.Channel()
	wo.sch = senbSensor
	failOnError(err, "Failed to open sendSensor channel")

	go func() {
		log.Println("Ready wait for 'sensor' events for", w.AID, w.ID)
		for {
			var aid, sid string
			var data map[string]interface{}
			select {
			case <-wo.closed:
				return
			case d := <-msgs:
				rk := strings.Split(d.RoutingKey, ".")
				aid, sid = rk[0], rk[1]

				err := json.Unmarshal(d.Body, &data)
				if err != nil {
					log.Printf("fail to unmarshal sensor body: %v, CORRID=%s", err, d.CorrelationId)
					continue
				}
			case d := <-oldSensors:
				var ok bool
				sid, ok = d["sid"].(string)
				if !ok {
					continue
				}
				aid, ok = d["aid"].(string)
				if sid == "" {
					continue
				}
				data = d["data"].(map[string]interface{})
			}

			msgTime, ok := data["created_at"].(string)
			if !ok {
				log.Printf("missing created_at field in sensors %v.%v\n", aid, sid)
			}
			recvTime, err := time.Parse(time.RFC3339Nano, msgTime)
			if !ok {
				log.Printf("created_at parsing error in sensors %v.%v: %v\n", aid, sid, err)
			}

			// the message is not need, nothing to do here
			recompute, err := w.SendInput(aid, sid, recvTime, data)
			if err != nil {
				log.Printf("error in the input sensors %s to workflow %s.%s: %v", sid, aid, w.ID, err)
				continue
			}
			if !recompute {
				continue
			}

			// update the graph value
			w.Compute()

			// make a set of message id where at least one of its field has changed
			changed := make(map[string]struct{})
			for _, out := range w.Output() {
				if out.HasChanged() {
					changed[out.ID] = struct{}{}
				}
			}
			output := make(map[string]map[string]interface{})
			for _, out := range w.Output() {
				// only contruct message if its in the changed set of message id
				if _, mustSend := changed[out.ID]; !mustSend {
					continue
				}
				v, exist := output[out.ID]
				if !exist {
					// construct the message
					v = make(map[string]interface{})
					v["id"] = out.ID
					output[out.ID] = v
				}
				// append the field to the message
				v[out.Name] = out.ComputedValue
			}

			now := time.Now().Format(time.RFC3339Nano)

			// send each output messages
			for sid, out := range output {
				out["created_at"] = now
				body, err := json.Marshal(out)
				if err != nil {
					log.Println(err)
					continue
				}

				log.Printf("send %v.%v: %s\n", aid, sid, body)

				err = senbSensor.Publish(
					"sensors",   // exchange
					aid+"."+sid, // routing key
					false,       // mandatory
					false,       // immediate
					amqp.Publishing{
						DeliveryMode: amqp.Persistent,
						// CorrelationId: coorID,
						Headers:     amqp.Table{"timestamp": now},
						ContentType: "application/json",
						Body:        body,
					})
				if err != nil {
					log.Println(err)
				}

				// send webhook if it exist
				if url, exist := w.Hooks[sid]; exist {
					go func() {
						resp, err := http.Post(url, "application/json", bytes.NewReader(body))
						if err != nil {
							log.Printf("cannot send webhood for %s.%s: %v\n", aid, sid, err)
							return
						}
						defer resp.Body.Close()
						if resp.StatusCode/100 != 2 {
							log.Printf("cannot send webhood for %s.%s: %v\n", aid, sid, resp.Status)
							return
						}
						log.Printf("send webhook %v.%v to %s\n", aid, sid, url)
					}()
				}
			}
		}
	}()

	return wo, nil
}

func openCommandChannel(conn *amqp.Connection, name string) *amqp.Channel {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		"workflow", // name
		"topic",    // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	return ch
}

func (h *handler) handleIncomingCommand() {
	ch, err := h.conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		"workflow", // name
		"topic",    // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Get every routing key
	err = ch.QueueBind(
		q.Name,     // queue name
		"#",        // routing key
		"workflow", // exchange
		false,      // no wait
		nil,        // args
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name,       // queue
		h.workerName, // consumer
		true,         // auto ack
		true,         // exclusive
		false,        // no local
		false,        // no wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	log.Println("Ready wait for 'workflow' command events")

	for d := range msgs {
		rk := strings.Split(d.RoutingKey, ".")
		if len(rk) < 4 {
			log.Println("missing command", d.RoutingKey)
			d.Ack(false)
			continue
		}
		worker := rk[0]
		aid := rk[1]
		wid := rk[2]
		cmd := rk[3]
		switch cmd {
		case "start":
			if worker != h.workerName {
				if has := h.stopWorkflow(wid); has {
					log.Printf("STOP workflow %s.%s moved to %s", aid, wid, worker)
				}
				continue
			}

			var w dbWorkflow
			err := json.Unmarshal(d.Body, &w)
			if err != nil {
				log.Printf("from workflow topic, could not unmarshal workflow %s.%s: %v", aid, wid, err)
				continue
			}
			if err := h.startWorkflow(w); err != nil {
				log.Printf("from workflow topic, could not start workflow %s.%s: %v", aid, wid, err)
				continue
			}

		case "stop":
			if h.stopWorkflow(wid) {
				log.Printf("from workflow topic STOP workflow %s.%s", aid, wid)
			}
		}
	}
}

func (h *handler) sendStarCommand(w dbWorkflow) error {
	body, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return h.sendCmdCh.Publish(
		"workflow", // exchange
		fmt.Sprintf("%s.%s.%s.start", w.Worker, w.AccountID, w.ID), // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:      amqp.Table{"timestamp": time.Now().Format(time.RFC3339Nano)},
			ContentType:  "application/json",
			Body:         body,
		})
}

func (h *handler) sendStopCommand(acoundID, workflowID string) error {
	return h.sendCmdCh.Publish(
		"workflow", // exchange
		fmt.Sprintf(".%s.%s.stop", acoundID, workflowID), // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Headers:      amqp.Table{"timestamp": time.Now().Format(time.RFC3339Nano)},
		})
}

func (h *handler) handleAccountDeleted() {
	ch, err := h.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"account", // name
		"fanout",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a account exchange")

	q, err := ch.QueueDeclare(
		"workflow-engine_account", // name
		true,                      // durable
		false,                     // delete when usused
		false,                     // exclusive
		false,                     // no-wait
		nil,                       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,     // queue name
		"#.delete", // routing key
		"account",  // exchange
		false,      // no-wait
		nil)
	if err != nil {
		log.Panicln(err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	log.Println("Ready, wait on topic 'account'")

	for d := range msgs {
		straid := strings.SplitN(d.RoutingKey, ".", 2)
		aid, err := uuid.FromString(straid[0])
		if err != nil {
			log.Printf("cannot extract aid from routing key %v: %v\n", d.RoutingKey, err)
			d.Ack(false)
			continue
		}
		log.Printf("purge sensors for account %v\n", aid)

		query := "SELECT DISTINCT id FROM workflow WHERE account_id = $1"
		rows, err := h.db.Query(query, aid)
		if err != nil && err != sql.ErrNoRows {
			d.Nack(false, true)
			failOnError(err, "could not retreive workflow to lunch")
		}
		for rows.Next() {
			var wid string
			lerr := rows.Scan(&wid)
			if lerr != nil {
				err = lerr
				log.Printf("could not scan workflow from db: %v", err)
				continue
			}
			h.sendStopCommand(aid.String(), wid)
		}

		_, err = h.db.Exec("DELETE FROM workflow WHERE account_id=$1;", aid)
		if err != nil {
			log.Printf("cannot purge %v: %v\n", aid, err)
		}
		d.Ack(false)
	}
}

func findLatestSensors(ctx context.Context, conn *amqp.Connection, aid uuid.UUID, corrID string) (<-chan map[string]interface{}, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	request := struct {
		AID uuid.UUID `json:"aid"`
	}{aid}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	err = ch.Publish(
		"",                   // exchange
		"rpc_sensors_latest", // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrID,
			ReplyTo:       q.Name,
			Body:          body,
		},
	)
	if err != nil {
		return nil, err
	}

	latests := make(chan map[string]interface{}, 16)

	go func() {
		defer func() {
			ch.QueueDelete(q.Name, false, false, false)
			ch.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				return

			case d := <-msgs:
				if corrID == d.CorrelationId {
					var s []map[string]interface{}
					err := json.Unmarshal(d.Body, &s)
					if err != nil {
						log.Println(err)
						return
					}
					d.Ack(false)
					for _, ms := range s {
						latests <- ms
					}
					return
				}
			}
		}
	}()

	return latests, nil
}
