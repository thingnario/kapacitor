package kapacitor

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-redis/redis"
	"github.com/thingnario/kapacitor/edge"
	"github.com/thingnario/kapacitor/keyvalue"
	"github.com/thingnario/kapacitor/models"
	"github.com/thingnario/kapacitor/pipeline"
)

type ChangeDetectNode struct {
	node
	d *pipeline.ChangeDetectNode
}

// Create a new changeDetect node.
func newChangeDetectNode(et *ExecutingTask, n *pipeline.ChangeDetectNode, d NodeDiagnostic) (*ChangeDetectNode, error) {
	dn := &ChangeDetectNode{
		node: node{Node: n, et: et, diag: d},
		d:    n,
	}
	// Create stateful expressions
	dn.node.runF = dn.runChangeDetect
	return dn, nil
}

func (n *ChangeDetectNode) runChangeDetect([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *ChangeDetectNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup()),
	), nil
}

func (n *ChangeDetectNode) newGroup() *changeDetectGroup {
	return &changeDetectGroup{
		n: n,
	}
}

type changeDetectGroup struct {
	n        *ChangeDetectNode
	previous edge.FieldsTagsTimeGetter
}

func (g *changeDetectGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	if s := begin.SizeHint(); s > 0 {
		begin = begin.ShallowCopy()
		begin.SetSizeHint(0)
	}
	g.previous = nil
	return begin, nil
}

func (g *changeDetectGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	changed := g.doChangeDetect(bp)
	if changed {
		return bp, nil
	}
	return nil, nil
}

func (g *changeDetectGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *changeDetectGroup) Point(p edge.PointMessage) (edge.Message, error) {
	if g.previous == nil {
		g.previous = p.ShallowCopy()
		key := fmt.Sprintf("changeDetectNode:%s:%s", g.n.et.Task.ID, p.GroupID())
		err := WriteToRedis(key, g.previous.Fields())
		if err != nil {
			fmt.Println("Oops!")
		}
	}
	changed := g.doChangeDetect(p)
	if changed {
		key := fmt.Sprintf("changeDetectNode:%s:%s", g.n.et.Task.ID, p.GroupID())
		WriteToRedis(key, p.Fields())
		return p, nil
	}
	return nil, nil
}

// doChangeDetect computes the changeDetect with respect to g.previous and p.
// The resulting changeDetect value will be set on n.
func (g *changeDetectGroup) doChangeDetect(p edge.FieldsTagsTimeGetter) bool {
	var prevFields, currFields models.Fields
	if g.previous != nil {
		prevFields = g.previous.Fields()
	}
	currFields = p.Fields()
	changed := g.n.changeDetect(prevFields, currFields)

	if !changed {
		return false
	}
	g.previous = p
	return true
}

func (g *changeDetectGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (g *changeDetectGroup) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (g *changeDetectGroup) Done() {}

// changeDetect reports whether there was a change between prev and cur.
func (n *ChangeDetectNode) changeDetect(prev, curr models.Fields) bool {
	for _, field := range n.d.Fields {
		value, ok := curr[field]
		if !ok {
			n.diag.Error("Invalid field in change detect",
				fmt.Errorf("expected field %s not found", field),
				keyvalue.KV("field", field))
			continue
		}
		if prev[field] == nil {
			continue
		}
		if prev[field] != value {
			return true
		}
	}
	return false
}

func WriteToRedis(key string, value map[string]interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	client := GetRedisInstance()
	return client.Set(key, b, 0).Err()
}

var redisClientInstance *redis.Client
var once sync.Once

func GetRedisInstance() *redis.Client {
	once.Do(func() {
		redisClientInstance = redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	})
	return redisClientInstance
}
