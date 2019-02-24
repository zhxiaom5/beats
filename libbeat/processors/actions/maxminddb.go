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
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/processors"
	"github.com/oschwald/maxminddb-golang"
    "encoding/json"
    "strconv"
    "io/ioutil"
    "net"
)

type maxmind struct {
	Field    string
	MmdbPath string
    ZonePath string
    ZoneData map[string]int
	Mmdb     *maxminddb.Reader
}

func init() {
	processors.RegisterPlugin("maxmind",
		configChecked(NewMaxmind,
			requireFields("field", "mmdbpath","zonepath"),
			allowedFields("field", "mmdbpath","zonepath", "when")))
}

func NewMaxmind(c *common.Config) (processors.Processor, error) {
	config := struct {
		Field    string `config:"field"`
		MmdbPath string `config:"mmdbpath"`
        ZonePath string `config:"zonepath"`
	}{}
	err := c.Unpack(&config)
	if err != nil {
		return nil, fmt.Errorf("fail to unpack the maxmind configuration: %s", err)
	}

	/* remove read only fields */
	for _, readOnly := range processors.MandatoryExportedFields {
		if config.Field == readOnly {
			return nil, fmt.Errorf("%s is a read only field, cannot override", readOnly)
		}
	}

	f := &maxmind{
		Field:    config.Field,
		MmdbPath: config.MmdbPath,
        ZonePath: config.ZonePath,
	}

    //open maxmind file 
	f.Mmdb, err = maxminddb.Open(f.MmdbPath)
	if err != nil {
		return nil, fmt.Errorf("fail to open maxmind file,path :%s,error: %s",f.MmdbPath, err)
	}

    //open and read zone file
    zone_raw_data, err := ioutil.ReadFile(f.ZonePath)
    if err != nil {
        return nil, fmt.Errorf("fail to open zone data file,path :%s,error: %s",f.ZonePath, err)
    }   

    err = json.Unmarshal(zone_raw_data,&f.ZoneData)
    if err != nil {
        return nil, fmt.Errorf("fail unmarshal zone raw data :%s,error: %s",zone_raw_data, err)
    }

	return f, nil
}

func (f *maxmind) Run(event *beat.Event) (*beat.Event, error) {
	fieldValue, err := event.GetValue(f.Field)
	if err != nil {
		return event, fmt.Errorf("error getting field '%s' from event", f.Field)
	}

	value, ok := fieldValue.(string)
	if !ok {
		return event, fmt.Errorf("could not get a string from field '%s'", f.Field)
	}
    
    ipaddr := net.ParseIP(value)

	//geoinfo
    var record struct {
        City int `maxminddb:"city"`
        Country int `maxminddb:"country"`
        Isp int `maxminddb:"isp"`
        Prov int `maxminddb:"prov"`
        Zone int `json:"-"`
    }  

    err = f.Mmdb.Lookup(ipaddr, &record)
    if err != nil {
       return event, fmt.Errorf("mmdblookup failed'%s'", f.Field)
    }

    //get Zone id
    if ( record.Prov > 0 && record.Prov <=34 ) {
        record.Zone = f.ZoneData[strconv.Itoa(record.Prov)]  
    }else{
        record.Zone = 0
    }

    
	//add geoinfo to value
    event.PutValue("geoinfo", record)
	return event, nil
}

func (f *maxmind) String() string {
	return "maxmind="+f.MmdbPath
}
