package eventbridge

import (
	"errors"

	"github.com/AirHelp/rabbit-amazon-forwarder/config"
	"github.com/AirHelp/rabbit-amazon-forwarder/forwarder"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/aws/aws-sdk-go/service/eventbridge/eventbridgeiface"
	log "github.com/sirupsen/logrus"
)

const (
	// Type forwarder type
	Type = "EventBridge"
)

// Forwarder forwarding client
type Forwarder struct {
	name              string
	eventBridgeClient eventbridgeiface.EventBridgeAPI
	function          string
}

// CreateForwarder creates instance of forwarder
func CreateForwarder(entry config.AmazonEntry, eventBridgeClient ...eventbridgeiface.EventBridgeAPI) forwarder.Client {
	var client eventbridgeiface.EventBridgeAPI
	if len(eventBridgeClient) > 0 {
		client = eventBridgeClient[0]
	} else {
		client = eventbridge.New(session.Must(session.NewSession()))
	}
	forwarder := Forwarder{entry.Name, client, entry.Target}
	log.WithField("forwarderName", forwarder.Name()).Info("Created forwarder")
	return forwarder
}

// Name forwarder name
func (f Forwarder) Name() string {
	return f.name
}

// Push pushes message to forwarding infrastructure
func (f Forwarder) Push(message string) error {
	if message == "" {
		return errors.New(forwarder.EmptyMessageError)
	}

	// wrap in JSON
	message = `{"body":"` + message + `"}`

	// build event call
	source := f.Name()
	params := &eventbridge.PutEventsInput{
		Entries: []*eventbridge.PutEventsRequestEntry{{
			Source: &source,
			Detail: &message,
		}},
	}

	// do put and check response
	resp, eventsOut := f.eventBridgeClient.PutEventsRequest(params)
	if resp.Error != nil {
		log.WithFields(log.Fields{
			"forwarderName": f.Name(),
			"requestError":  resp.Error}).Errorf("Could not forward message")
		return resp.Error
	}

	if eventsOut.FailedEntryCount != nil && *eventsOut.FailedEntryCount > 0 {
		for _, entry := range eventsOut.Entries {
			if entry.ErrorCode != nil {
				if entry.ErrorMessage != nil {
					log.WithFields(log.Fields{
						"forwarderName": f.Name(),
						"error":         entry.ErrorMessage}).Error("Could not forward message")
					return errors.New(*entry.ErrorMessage)
				}
				log.WithFields(log.Fields{
					"forwarderName": f.Name(),
					"error":         "Unexpected error"}).Error("Could not forward message")
				return errors.New("Unexpected error")
			}
			log.WithFields(log.Fields{
				"forwarderName": f.Name(),
				"error":         "Unexpected error"}).Error("Could not forward message")
			return errors.New("Unexpected error")
		}
	}

	// log success
	log.WithFields(log.Fields{
		"forwarderName": f.Name()}).Info("Forward succeeded")
	return nil
}
