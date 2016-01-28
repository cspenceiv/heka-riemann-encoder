/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012-2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#   Mike Trinkala (trink@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package heka_rieman_encoder

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/cspenceiv/heka-riemann-encoder/riemenc"
	rproto "github.com/cspenceiv/heka-riemann-encoder/riemenc/proto"
)

// Encoder for converting Message objects into Protocol Buffer data.
type RiemannEncoder struct {
	processMessageCount    int64
	processMessageFailures int64
	processMessageSamples  int64
	processMessageDuration int64
	pConfig                *pipeline.PipelineConfig
	reportLock             sync.Mutex
	sample                 bool
	sampleDenominator      int
}

// Heka will call this before calling any other methods to give us access to
// the pipeline configuration.
func (re *RiemannEncoder) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	re.pConfig = pConfig
}

func (re *RiemannEncoder) Init(config interface{}) error {
	re.sample = true
	re.sampleDenominator = re.pConfig.Globals.SampleDenominator
	return nil
}

//func (re *RiemannEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
func (re *RiemannEncoder) Encode(pack *pipeline.PipelinePack) ([]byte, error) {
	atomic.AddInt64(&re.processMessageCount, 1)
	var startTime time.Time
	if re.sample {
		startTime = time.Now()
	}

	// Gather pack data into riemann event struct
	var event = &riemenc.Event{}
	var message = pack.Message
	event.Time = *message.Timestamp / 1e9
	
	if nil != message.Severity {
		switch int(*message.Severity) {
			case 0:
				event.State = "Emergency"
			case 1:
				event.State = "Alert"
			case 2:
				event.State = "Critical"
			case 3:
				event.State = "Error"
			case 4:
				event.State = "Warning"
			case 5:
				event.State = "Notice"
			case 6:
				event.State = "Informational"
			case 7:
				event.State = "Debug"
		}
	}
	
	event.Service = *message.Logger //Consider the use of Logger and Type
	event.Host = *message.Hostname
	if len(*message.Payload) != 0 {
		event.Description = *message.Payload
	}
	//event.Tags
	//event.Ttl
	//event.Attributes
	if metric, ok := message.GetFieldValue("Metric"); ok {
		event.Metric = metric
	}
	
	// End of gathering pack
	
	pbevent, err := riemenc.EventToPbEvent(event)
	if err != nil {
		return nil, err
	}
	
	msg := &rproto.Msg{}
	msg.Events = append(msg.Events, pbevent)
	pboutput, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// Prepend output with the length of pboutput for Riemann
	pblen := uint32(len(pboutput))
	output := make([]byte, 4)
	binary.BigEndian.PutUint32(output, pblen)
	output = append(output, pboutput[:]...)
	
	// Once the reimplementation of the output API is finished we should be
	// able to just return pack.MsgBytes directly, but for now we need to copy
	// the data to prevent problems in case the pack is zeroed and/or reused
	// (overwriting the pack.MsgBytes memory) before we're done with it.
	//output = make([]byte, len(pack.MsgBytes))
	//copy(output, pack.MsgBytes)

	if re.sample {
		duration := time.Since(startTime).Nanoseconds()
		re.reportLock.Lock()
		re.processMessageDuration += duration
		re.processMessageSamples++
		re.reportLock.Unlock()
	}
	re.sample = 0 == rand.Intn(re.sampleDenominator)
	return output, nil
}

func (re *RiemannEncoder) Stop() {
	return
}

func (re *RiemannEncoder) ReportMsg(msg *message.Message) error {
	re.reportLock.Lock()
	defer re.reportLock.Unlock()

	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&re.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&re.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageSamples",
		re.processMessageSamples, "count")

	var tmp int64 = 0
	if re.processMessageSamples > 0 {
		tmp = re.processMessageDuration / re.processMessageSamples
	}
	message.NewInt64Field(msg, "ProcessMessageAvgDuration", tmp, "ns")

	return nil
}

func init() {
	pipeline.RegisterPlugin("RiemannEncoder", func() interface{} {
		return new(RiemannEncoder)
	})
}
