package pilot

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

// RequestNotification for REQ_UPDATED, REQ_ADDED, REQ_DELETED
type RequestNotification struct {
	MessageType   string
	RequestID     string
	MatID         string
	Destination   string
	RequestorName string `json:"-"`
	Status        string
	Blank         string `json:"-"`
	Hostname      string `json:"-"`
}

// TransferUpdatedNotification for TRAN_UPDATED
type TransferUpdatedNotification struct {
	MessageType string
	RequestID   string
	TransferID  string
	Source      string
	Destination string
	Status      string
	Hostname    string `json:"-"`
}

// TransferProgressNotification for TRAN_PROGRESS
type TransferProgressNotification struct {
	MessageType string
	RequestID   string
	TransferID  string
	Progress    string
	Status      string
	Hostname    string `json:"-"`
}

// ToRequestJSON transforms a flat pilot message string into an equivalent JSON byte array
// to account for MED-9831, could return multiple logical messages
func ToRequestJSON(message string) (out map[string]string, err error) {
	isPilot := regexp.MustCompile(`%BRONOT[^%]+%`)
	pilotMessages := isPilot.FindAllString(message, -1) // -1 to return all matches
	if len(pilotMessages) < 1 {
		return nil, errors.New("Invalid message: " + message)
	}

	out = map[string]string{}
	for _, message := range pilotMessages {
		message = strings.TrimPrefix(message, `%BRONOT '`)
		message = strings.TrimSuffix(message, `'%`)
		splits := strings.Split(message, `','`)

		if len(splits) < 3 {
			return nil, errors.New("Not enough fields: " + message)
		}

		if splits[0] == "REQUEST" {
			log.Debug("Transforming " + splits[2] + " to JSON")

			switch splits[2] {
			case "REQ_UPDATED":
				fallthrough
			case "REQ_ADDED":
				fallthrough
			case "REQ_DELETED":
				if len(splits) < 10 {
					return nil, errors.New("Malformed " + splits[2] + " message, not enough fields: " + message)
				}
				jsonBytes, err := json.Marshal(&RequestNotification{
					MessageType:   splits[2],
					RequestID:     splits[3],
					MatID:         splits[4],
					Destination:   splits[5],
					RequestorName: splits[6],
					Status:        splits[7],
					Blank:         splits[8],
					Hostname:      splits[9],
				})
				if err != nil {
					return nil, err
				}
				out[splits[3]] = string(jsonBytes)
			case "TRAN_UPDATED":
				if len(splits) < 9 {
					return nil, errors.New("Malformed TRAN_UPDATED message, not enough fields: " + message)
				}
				jsonBytes, err := json.Marshal(&TransferUpdatedNotification{
					MessageType: splits[2],
					RequestID:   splits[3],
					TransferID:  splits[4],
					Source:      splits[5],
					Destination: splits[6],
					Status:      splits[7],
					Hostname:    splits[8],
				})
				if err != nil {
					return nil, err
				}
				out[splits[3]] = string(jsonBytes)
			case "TRAN_PROGRESS":
				if len(splits) < 8 {
					return nil, errors.New("Malformed TRAN_PROGRESS message, not enough fields: " + message)
				}
				jsonBytes, err := json.Marshal(&TransferProgressNotification{
					MessageType: splits[2],
					RequestID:   splits[3],
					TransferID:  splits[4],
					Progress:    splits[5],
					Status:      splits[6],
					Hostname:    splits[7],
				})
				if err != nil {
					return nil, err
				}
				out[splits[3]] = string(jsonBytes)
			default:
				return nil, errors.New("Malformed transfer message: " + message)
			}
		} else {
			return nil, errors.New("Non-request notification: " + message)
		}
	}

	return out, err
}
