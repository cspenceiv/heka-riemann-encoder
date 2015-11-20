package riemenc

import (
	"fmt"
	"reflect"

	pb "github.com/gogo/protobuf/proto"
	"github.com/cspenceiv/heka-riemann-encoder/riemenc/proto"
)

type Event struct {
	Time		int64
	State		string
	Service		string
	Host		string
	Description	string
	Tags		[]string
	Ttl			float32
	Attributes	map[string]string
}

// Code taken from github.com/bigdatadev/goryman
func EventToPbEvent(event *Event) (*proto.Event, error) {
	var e proto.Event
	t := reflect.ValueOf(&e).Elem()
	s := reflect.ValueOf(event).Elem()
	typeOfEvent := s.Type()

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		value := reflect.ValueOf(f.Interface())
		if reflect.Zero(f.Type()) != value && f.Interface() != nil {
			name := typeOfEvent.Field(i).Name
			switch name {
			case "State", "Service", "Host", "Description":
				tmp := reflect.ValueOf(pb.String(value.String()))
				t.FieldByName(name).Set(tmp)
			case "Ttl":
				tmp := reflect.ValueOf(pb.Float32(float32(value.Float())))
				t.FieldByName(name).Set(tmp)
			case "Time":
				tmp := reflect.ValueOf(pb.Int64(value.Int()))
				t.FieldByName(name).Set(tmp)
			case "Tags":
				tmp := reflect.ValueOf(value.Interface().([]string))
				t.FieldByName(name).Set(tmp)
			case "Metric":
				switch reflect.TypeOf(f.Interface()).Kind() {
				case reflect.Int:
					tmp := reflect.ValueOf(pb.Int64(int64(value.Int())))
					t.FieldByName("MetricSint64").Set(tmp)
				case reflect.Float32:
					tmp := reflect.ValueOf(pb.Float32(float32(value.Float())))
					t.FieldByName("MetricF").Set(tmp)
				case reflect.Float64:
					tmp := reflect.ValueOf(pb.Float64(value.Float()))
					t.FieldByName("MetricD").Set(tmp)
				default:
					return nil, fmt.Errorf("Metric of invalid type (type %v)",
						reflect.TypeOf(f.Interface()).Kind())
				}
			case "Attributes":
				var attrs []*proto.Attribute
				for k, v := range value.Interface().(map[string]string) {
					k_, v_ := k, v
					attrs = append(attrs, &proto.Attribute{
						Key:   &k_,
						Value: &v_,
					})
				}
				t.FieldByName(name).Set(reflect.ValueOf(attrs))
			}
		}
	}
	return &e, nil
}

