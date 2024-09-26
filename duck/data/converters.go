package data

import (
	"time"

	"github.com/araddon/dateparse"
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

// TODO: just define converters for the date fields we find
// then we can avoid looping through all the results and fields here
func ConvertDateFields(results []map[string]any) {
	dateFields := findDateFields(results)
	for row, result := range results {
		for key, value := range result {
			isDateField := dateFields[key]
			if isDateField != nil && *isDateField {
				if s, ok := value.(string); ok {
					dateValue := isDate(s)
					results[row][key] = dateValue
				}
			}
		}
	}
}

func findDateFields(results []map[string]any) map[string]*bool {
	dateFields := make(map[string]*bool)
	for _, result := range results {
		if len(dateFields) == len(result) {
			break
		}
		for key, value := range result {
			if value == nil {
				continue
			}

			isDateField := dateFields[key]
			if isDateField != nil {
				continue
			}

			if s, ok := value.(string); ok {
				if s == "" {
					continue
				}

				dateValue := isDate(s)
				flag := true
				flagPtr := &flag
				if dateValue != nil {
					dateFields[key] = flagPtr
				} else {
					*flagPtr = false
				}
			}
		}
	}
	return dateFields
}

func isDate(s string) *time.Time {
	val, err := dateparse.ParseStrict(s)
	if err != nil {
		return nil
	}
	return &val
}
