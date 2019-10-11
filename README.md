# Workflow Engine

Define `WORKER_NAME` env to affect the workflow to the right worker.

## Compile & run

    go build
    ./workflow-engine

## Exemple config in Environment variables

``` sh
PORT=8080
WORKER_NAME: "workflow-engine0"
AUTH_CHECK_URI="http://auth:1234/auth/check"
RABBITMQ_URI="amqp://guest:guest@rabbit:5672"
POSTGRESQL_URI="postgresql://postgres:passwords@localhost:5432/yic_workflow?sslmode=disable"
```

## Exemple of workflow JSON

```json
{
  "id": "0b034fe4-003c-4cda-9fcf-5040ac4d3ac1",
  "created_at": "2019-02-20T17:48:31.54862Z",
  "accound_id": "74b29b98-f9b3-43a5-86fb-0999c7078b9e",
  "worker": "workflow-engine0",
  "name": "Moulin flow",
  "version": "",
  "graph": {
    "cpu": {
      "id": "1377959e-97ce-46c1-9715-22c34bb9afbe",
      "name": "loadaverage",
      "type": "float",
      "operator": "input"
    },
    "ncpu": {
      "id": "1377959e-97ce-46c1-9715-22c34bb9afbe",
      "name": "numcpu",
      "type": "float",
      "operator": "input",
      "description": "Number of CPUs of the server"
    },
    "const0": {
      "type": "float",
      "operator": "const",
      "ComputedValue": 0.8
    },
    "status": {
      "id": "1377959e-97ce-46c1-9715-22c34bb9afbe",
      "name": "status",
      "type": "enum",
      "operator": "input"
    },
    "rel_cpu": {
      "inputs": [
        "cpu",
        "ncpu"
      ],
      "operator": "div"
    },
    "isfailure": {
      "inputs": [
        "status",
        "const_failure"
      ],
      "operator": "contains_exactly"
    },
    "isoffline": {
      "inputs": [
        "status",
        "const_offline"
      ],
      "operator": "contains_exactly"
    },
    "const_stop": {
      "type": "string",
      "operator": "const",
      "ComputedValue": "stop"
    },
    "const_broken": {
      "type": "string",
      "operator": "const",
      "ComputedValue": "broken"
    },
    "const_failure": {
      "type": "string",
      "operator": "const",
      "ComputedValue": "failure"
    },
    "const_offline": {
      "type": "string",
      "operator": "const",
      "ComputedValue": "offline"
    },
    "propeler_stop": {
      "inputs": [
        "isoffline",
        "const_stop",
        "propeler_broken"
      ],
      "operator": "match_str"
    },
    "windmill_fire": {
      "id": "cd0a6b8a-a32f-4cec-bd4d-38b24ac793e0",
      "name": "on_fire",
      "type": "enum",
      "inputs": [
        "isfailure"
      ],
      "operator": "output"
    },
    "windmill_grey": {
      "id": "cd0a6b8a-a32f-4cec-bd4d-38b24ac793e0",
      "name": "grey",
      "type": "bool",
      "inputs": [
        "isoffline"
      ],
      "operator": "output"
    },
    "propeler_broken": {
      "inputs": [
        "isfailure",
        "const_broken",
        "cpu_propeler_speed"
      ],
      "operator": "match_str"
    },
    "cpu_propeler_speed": {
      "inputs": [
        "rel_cpu"
      ],
      "values": [
        "stop",
        "slow",
        "medium",
        "fast"
      ],
      "operator": "select",
      "condition": [
        "0:.001",
        ".001:.3",
        ".3:.6",
        ".6:1"
      ]
    },
    "windmill_propeler_speed": {
      "id": "cd0a6b8a-a32f-4cec-bd4d-38b24ac793e0",
      "name": "propeler",
      "type": "enum",
      "inputs": [
        "propeler_stop"
      ],
      "values": [
        "stop",
        "slow",
        "medium",
        "fast",
        "broken"
      ],
      "operator": "output"
    }
  }
}
```
