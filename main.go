package main

import (
    "encoding/json"
    "io/ioutil"
    "net/http"
    "os"
    "time"

    "github.com/Sirupsen/logrus"
    "github.com/prometheus/client_golang/prometheus"
)

const (
    namespace  = "rabbitmq"
    defaultConfigPath = "config.json"
)

var log = logrus.New()

// Listed available metrics
var (
    connectionsTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "connections_total",
            Help:      "Total number of open connections.",
        },
        []string{
            // Which node was checked?
            "node",
        },
    )
    channelsTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "channels_total",
            Help:      "Total number of open channels.",
        },
        []string{
            "node",
        },
    )
    queuesTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "queues_total",
            Help:      "Total number of queues in use.",
        },
        []string{
            "node",
        },
    )
    consumersTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "consumers_total",
            Help:      "Total number of message consumers.",
        },
        []string{
            "node",
        },
    )
    exchangesTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "exchanges_total",
            Help:      "Total number of exchanges in use.",
        },
        []string{
            "node",
        },
    )
    messagesTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: namespace,
            Name:      "messages_total",
            Help:      "Total number of messages in all queues.",
        },
        []string{
            "node",
        },
    )
    messagesCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "messages",
            Name:      "messages",
            Help:      "Counter of messages.",
        },
        []string{
            "node",
        },
    )
    messagesReadyCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "messages",
            Name:      "messages_ready",
            Help:      "Counter of ready messages.",
        },
        []string{
            "node",
        },
    )
    messagesUnacknowledgedCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "messages",
            Name:      "messages_unacknowledged",
            Help:      "Counter of unacknowledged messages.",
        },
        []string{
            "node",
        },
    )
    messageStatsPublishCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "message_stats",
            Name:      "messages_published",
            Help:      "Counter of published messages.",
        },
        []string{
            "node",
        },
    )
    messageStatsAckCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "message_stats",
            Name:      "messages_acked",
            Help:      "Counter of acked messages.",
        },
        []string{
            "node",
        },
    )
    messageStatsDeliverCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "message_stats",
            Name:      "messages_delivered",
            Help:      "Counter of delivered messages.",
        },
        []string{
            "node",
        },
    )
    messageStatsConfirmCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "message_stats",
            Name:      "messages_confirmed",
            Help:      "Counter of confirmed messages.",
        },
        []string{
            "node",
        },
    )
    messageStatsRedeliverCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "message_stats",
            Name:      "messages_redelivered",
            Help:      "Counter of redelivered messages.",
        },
        []string{
            "node",
        },
    )
    messageStatsDeliverGetCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "message_stats",
            Name:      "messages_delivered_get",
            Help:      "Counter of delivered get messages.",
        },
        []string{
            "node",
        },
    )
    messageStatsDeliverNoAckCounter = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: namespace,
            Subsystem: "message_stats",
            Name:      "messages_delivered_no_ack",
            Help:      "Counter of delivered no ack messages.",
        },
        []string{
            "node",
        },
    )
)

type Config struct {
    Nodes    *[]Node `json:"nodes"`
    Port     string  `json:"port"`
    Interval string  `json:"req_interval"`
}

type Node struct {
    Name     string `json:"name"`
    Url      string `json:"url"`
    Uname    string `json:"uname"`
    Password string `json:"password"`
    Interval string `json:"req_interval,omitempty"`
}

func sendApiRequest(hostname, username, password, query string) *json.Decoder {
    client := &http.Client{}
    req, err := http.NewRequest("GET", hostname+query, nil)
    req.SetBasicAuth(username, password)

    resp, err := client.Do(req)

    if err != nil {
        log.Error(err)
        panic(err)
    }
    return json.NewDecoder(resp.Body)
}

func getOverview(hostname, username, password string) {
    decoder := sendApiRequest(hostname, username, password, "/api/overview")
    response := decodeObj(decoder)

    objectTotals := make(map[string]float64)
    for k, v := range response["object_totals"].(map[string]interface{}) {
        objectTotals[k] = v.(float64)
    }

    queueTotals := make(map[string]float64)
    for k, v := range response["queue_totals"].(map[string]interface{}) {
        switch v.(type) {
        case float64:
            queueTotals[k] = v.(float64)
        }
    }

    messageStats := make(map[string]float64)
    for k, v := range response["message_stats"].(map[string]interface{}) {
        switch v.(type) {
        case float64:
            messageStats[k] = v.(float64)
        }
    }

    nodename, _ := response["node"].(string)

    channelsTotal.WithLabelValues(nodename).Set(objectTotals["channels"])
    connectionsTotal.WithLabelValues(nodename).Set(objectTotals["connections"])
    consumersTotal.WithLabelValues(nodename).Set(objectTotals["consumers"])
    queuesTotal.WithLabelValues(nodename).Set(objectTotals["queues"])
    exchangesTotal.WithLabelValues(nodename).Set(objectTotals["exchanges"])
    messagesCounter.WithLabelValues(nodename).Set(queueTotals["messages"])
    messagesReadyCounter.WithLabelValues(nodename).Set(queueTotals["messages_ready"])
    messagesUnacknowledgedCounter.WithLabelValues(nodename).Set(queueTotals["messages_unacknowledged"])
    messageStatsRedeliverCounter.WithLabelValues(nodename).Set(messageStats["redeliver"])
    messageStatsConfirmCounter.WithLabelValues(nodename).Set(messageStats["confirm"])
    messageStatsDeliverCounter.WithLabelValues(nodename).Set(messageStats["deliver"])
    messageStatsPublishCounter.WithLabelValues(nodename).Set(messageStats["publish"])
    messageStatsAckCounter.WithLabelValues(nodename).Set(messageStats["ack"])
    messageStatsDeliverGetCounter.WithLabelValues(nodename).Set(messageStats["deliver_get"])
    messageStatsDeliverNoAckCounter.WithLabelValues(nodename).Set(messageStats["deliver_no_ack"])
}

func getNumberOfMessages(hostname, username, password string) {
    decoder := sendApiRequest(hostname, username, password, "/api/queues")
    response := decodeObjArray(decoder)
    nodename := response[0]["node"].(string)

    total_messages := 0.0
    for _, v := range response {
        total_messages += v["messages"].(float64)
    }
    messagesTotal.WithLabelValues(nodename).Set(total_messages)
}

func decodeObj(d *json.Decoder) map[string]interface{} {
    var response map[string]interface{}

    if err := d.Decode(&response); err != nil {
        log.Error(err)
    }
    return response
}

func decodeObjArray(d *json.Decoder) []map[string]interface{} {
    var response []map[string]interface{}

    if err := d.Decode(&response); err != nil {
        log.Error(err)
    }
    return response
}

func updateNodesStats(config *Config) {
    for _, node := range *config.Nodes {

        if len(node.Interval) == 0 {
            node.Interval = config.Interval
        }
        go runRequestLoop(node)
    }
}

func requestData(node Node) {
    defer func() {
        if r := recover(); r != nil {
            dt := 10 * time.Second
            time.Sleep(dt)
        }
    }()

    getOverview(node.Url, node.Uname, node.Password)
    getNumberOfMessages(node.Url, node.Uname, node.Password)

    dt, err := time.ParseDuration(node.Interval)
    if err != nil {
        log.Warn(err)
        dt = 30 * time.Second
    }
    time.Sleep(dt)
}

func runRequestLoop(node Node) {
    for {
        requestData(node)
    }
}

func loadConfig(path string, c *Config) bool {
    defer func() {
        if r := recover(); r != nil {
            dt := 10 * time.Second
            time.Sleep(dt)
        }
    }()

    file, err := ioutil.ReadFile(path)
    if err != nil {
        log.Error(err)
        panic(err)
    }

    err = json.Unmarshal(file, c)
    if err != nil {
        log.Error(err)
        panic(err)
    }
    return true
}

func runLoadConfigLoop(path string, c *Config) {
    for {
        is_ok := loadConfig(path, c)
        if is_ok == true {
            break
        }
    }
}

func main() {
    configPath := defaultConfigPath
    if len(os.Args) > 1 {
        configPath = os.Args[1]
    }
    log.Out = os.Stdout

    var config Config

    runLoadConfigLoop(configPath, &config)
    updateNodesStats(&config)

    http.Handle("/metrics", prometheus.Handler())
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        w.Write([]byte(`<html>
             <head><title>RabbitMQ Exporter</title></head>
             <body>
             <h1>RabbitMQ Exporter</h1>
             <p><a href='/metrics'>Metrics</a></p>
             </body>
             </html>`))
    })
    log.Infof("Starting RabbitMQ exporter on port: %s.", config.Port)
    http.ListenAndServe(":"+config.Port, nil)
}

// Register metrics to Prometheus
func init() {
    prometheus.MustRegister(channelsTotal)
    prometheus.MustRegister(connectionsTotal)
    prometheus.MustRegister(queuesTotal)
    prometheus.MustRegister(exchangesTotal)
    prometheus.MustRegister(consumersTotal)
    prometheus.MustRegister(messagesTotal)
    prometheus.MustRegister(messagesCounter)
    prometheus.MustRegister(messagesReadyCounter)
    prometheus.MustRegister(messageStatsAckCounter)
    prometheus.MustRegister(messageStatsDeliverCounter)
    prometheus.MustRegister(messageStatsRedeliverCounter)
    prometheus.MustRegister(messageStatsConfirmCounter)
    prometheus.MustRegister(messageStatsPublishCounter)
    prometheus.MustRegister(messageStatsDeliverNoAckCounter)
    prometheus.MustRegister(messageStatsDeliverGetCounter)
}
