package data

import (
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/framestruct"
)

func Converters(frames []*data.Frame) []framestruct.FramestructOption {
	var convs = []framestruct.FramestructOption{}
	var fields = map[string]*data.Field{}
	for _, f := range frames {
		for _, fld := range f.Fields {
			fields[fld.Name] = fld
		}
	}
	for _, fld := range fields {
		conv := converterMap[fld.Type()]
		if conv != nil {
			converter := framestruct.WithConverterFor(fld.Name, conv)
			convs = append(convs, converter)
		}
	}
	return convs
}

var converterMap = map[data.FieldType]func(i interface{}) (interface{}, error){
	data.FieldTypeTime:         timeConverter,
	data.FieldTypeNullableTime: timeConverter,
}

var timeConverter = func(i interface{}) (interface{}, error) {
	if s, ok := i.(string); ok {
		return parseDate(s)
	}
	if s, ok := i.(*string); ok {
		return parseDate(*s)
	}
	return nil, nil
}

const layout = "2006-01-02 15:04:05-07"

func parseDate(s string) (time.Time, error) {
	t, err := time.Parse(layout, s)
	if err != nil {
		logger.Error("failed to parse time", "error", err)
		return t, err
	}
	return t.UTC(), nil
}
