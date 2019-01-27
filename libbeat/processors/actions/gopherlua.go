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
	"strings"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
    "github.com/yuin/gopher-lua"
	"layeh.com/gopher-luar"
	"github.com/yuin/gluamapper"
)


type GopherLuaEngine struct {
	File   string
	Engine *lua.LState
	logger *logp.Logger
}

func init() {
	processors.RegisterPlugin("gopherlua",
		configChecked(newGopherLuaEngine,
			requireFields("file"),
			allowedFields("file", "when")))

}

func newGopherLuaEngine(c *common.Config) (processors.Processor, error) {
	config := struct {
		File string `config:"file"`
	}{}
	logger := logp.NewLogger("lua")
	err := c.Unpack(&config)
	if err != nil {
		return nil, fmt.Errorf("fail to unpack the configuration: %s", err)
	}

	engine := lua.NewState()
	defer engine.Close()

	if err := engine.DoFile(config.File); err != nil {
		logger.Warn("lua file read  error occured ", err)
	}

	/* remove read only Patterns */
	/*  for _, readOnly := range processors.MandatoryExportedFields {
	        for i, field := range config.File {
	            if readOnly == field {
	                config.Patterns = append(config.Patterns[:i], config.Patterns[i+1:]...)
	            }
	        }
	    }
	    g, _ := grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true})
	*/
	f := &GopherLuaEngine{File: config.File,
		Engine: engine,
		logger: logger}
	return f, nil
}

func (f *GopherLuaEngine) Run(event *beat.Event) (*beat.Event, error) {
	var errors []string

	//convert beat.event
	luaParam := luar.New(f.Engine, event)

	if err := f.Engine.CallByParam(lua.P{
		Fn:      f.Engine.GetGlobal("process"),
		NRet:    1,
		Protect: true,
	}, luaParam); err != nil {
		f.logger.Warn("call lua file func error", err)
	}
	//returned value
	ret := f.Engine.Get(-1)
	f.Engine.Pop(1)

    addFields:= make(map[string]interface{})

	luatable, ok := ret.(*lua.LTable)
	if ok {
		if err := gluamapper.Map(luatable,&addFields); err == nil {
            for kkk, vvv := range addFields {
            	event.PutValue(kkk, vvv)
        	} 
		}else{
			f.logger.Warn("lua table cant convert to map[string]interface{}", ret)
		}
	} else {
		f.logger.Warn("cannot convert variable,lua funct must return LTable", ret)
	}

	if len(errors) > 0 {
		return event, fmt.Errorf(strings.Join(errors, ", "))
	}
	return event, nil
}

func (f *GopherLuaEngine) String() string {
	return "gopherlua=" + f.File
}
