// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package actions

import (
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/processors"
)

type unixms struct {
	//from which field to get
	Field string
	//the timezone
	Timezone string
	//add to what keyname
	KeyName string
}

func init() {
	processors.RegisterPlugin("unixms",
		configChecked(NewUnixMS,
			requireFields("field", "timezone", "keyname"),
			allowedFields("field", "timezone", "keyname", "when")))
}

func NewUnixMS(c *common.Config) (processors.Processor, error) {
	config := struct {
		Field    string `config:"field"`
		Timezone string `config:"timezone"`
		KeyName  string `config:"keyname"`
	}{}
	err := c.Unpack(&config)
	if err != nil {
		return nil, fmt.Errorf("fail to unpack the unixms configuration: %s", err)
	}

	/* remove read only fields */
	for _, readOnly := range processors.MandatoryExportedFields {
		if config.Field == readOnly {
			return nil, fmt.Errorf("%s is a read only field, cannot override", readOnly)
		}
	}

	f := &unixms{
		Field:    config.Field,
		Timezone: config.Timezone,
		KeyName:  config.KeyName,
	}
	return f, nil
}

func (f *unixms) Run(event *beat.Event) (*beat.Event, error) {
	fieldValue, err := event.GetValue(f.Field)
	if err != nil {
		return event, fmt.Errorf("error getting field '%s' from event", f.Field)
	}

	value, ok := fieldValue.(string)
	if !ok {
		return event, fmt.Errorf("could not get a string from field '%s'", f.Field)
	}

	//load timezone
	loc, err := time.LoadLocation(f.Timezone)
	if err != nil {
		return event, fmt.Errorf("error loading timezone '%s'", f.Timezone)
	}

	//format unknown date format to unixms
	t, err := dateparse.ParseIn(value, loc)
	if err != nil {
		return event, fmt.Errorf("could not convert  '%s' to unixms,maybe it not any dateformat", value)
	}

	// add unixms to event
	event.PutValue(f.KeyName, t.UnixNano()/1e6)
	return event, nil
}

func (f unixms) String() string {
	return "unixms=root"
}
