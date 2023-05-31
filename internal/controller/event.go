package controller

type eventType string

const (
	objectAdd    eventType = "ObjectAdd"
	objectUpdate eventType = "ObjectUpdate"
	objectDelete eventType = "ObjectDelete"
)

type event struct {
	eventType eventType
	objKey    string
}
