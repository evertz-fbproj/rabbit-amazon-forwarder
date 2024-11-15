package eventbridge

import (
	"errors"
	"testing"

	"github.com/AirHelp/rabbit-amazon-forwarder/config"
	"github.com/AirHelp/rabbit-amazon-forwarder/forwarder"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/aws/aws-sdk-go/service/eventbridge/eventbridgeiface"
)

const (
	badRequest     = "Bad request"
	handlerError   = "Handled"
	unhandledError = "Unhandled"
)

func TestCreateForwarder(t *testing.T) {
	entry := config.AmazonEntry{Type: "EventBridge",
		Name:   "eventbridge-test",
		Target: "function1-test",
	}
	forwarder := CreateForwarder(entry)
	if forwarder.Name() != entry.Name {
		t.Errorf("wrong forwarder name, expected:%s, found: %s", entry.Name, forwarder.Name())
	}
}

func TestPush(t *testing.T) {
	functionName := "function1-test"
	entry := config.AmazonEntry{Type: "EventBridge",
		Name:   "eventbridge-test",
		Target: functionName,
	}
	scenarios := []struct {
		name     string
		mock     eventbridgeiface.EventBridgeAPI
		message  string
		function string
		err      error
	}{
		{
			name:     "empty message",
			mock:     mockAmazonEventBridge{resp: eventbridge.PutEventsOutput{Entries: []*eventbridge.PutEventsResultEntry{{}}}, function: functionName, message: ""},
			message:  "",
			function: functionName,
			err:      errors.New(forwarder.EmptyMessageError),
		},
		{
			name: "unhandled error",
			mock: mockAmazonEventBridge{
				resp: eventbridge.PutEventsOutput{
					FailedEntryCount: aws.Int64(1),
					Entries: []*eventbridge.PutEventsResultEntry{{
						ErrorCode:    aws.String("1234"),
						ErrorMessage: aws.String(unhandledError),
					}},
				},
				function: functionName,
				message:  unhandledError,
			},
			message:  unhandledError,
			function: functionName,
			err:      errors.New(unhandledError),
		},
		{
			name:     "success",
			mock:     mockAmazonEventBridge{resp: eventbridge.PutEventsOutput{Entries: []*eventbridge.PutEventsResultEntry{{}}}, function: functionName, message: "abc"},
			message:  "abc",
			function: functionName,
			err:      nil,
		},
	}
	for _, scenario := range scenarios {
		t.Log("Scenario name: ", scenario.name)
		forwarder := CreateForwarder(entry, scenario.mock)
		err := forwarder.Push(scenario.message)
		if scenario.err == nil && err != nil {
			t.Errorf("Error should not occur. Error: %s", err.Error())
			return
		}
		if scenario.err == err {
			return
		}
		if err == nil {
			t.Errorf("Error should occur. Expected: %s", scenario.err.Error())
			return
		}
		if err.Error() != scenario.err.Error() {
			t.Errorf("Wrong error, expecting:%v, got:%v", scenario.err, err)
		}
	}
}

type mockAmazonEventBridge struct {
	eventbridgeiface.EventBridgeAPI
	resp     eventbridge.PutEventsOutput
	function string
	message  string
}

func (m mockAmazonEventBridge) PutEventsRequest(input *eventbridge.PutEventsInput) (*request.Request, *eventbridge.PutEventsOutput) {
	return &request.Request{}, &m.resp
}
