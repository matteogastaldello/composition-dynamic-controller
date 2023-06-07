package condition

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TypeReady         = "Ready"
	ReasonAvailable   = "Available"
	ReasonUnavailable = "Unavailable"
	ReasonCreating    = "Creating"
	ReasonDeleting    = "Deleting"
)

func Unavailable() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonUnavailable,
	}
}

// Creating returns a condition that indicates the resource is currently
// being created.
func Creating() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonCreating,
	}
}

func FailWithReason(reason string) metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
	}
}

// Deleting returns a condition that indicates the resource is currently
// being deleted.
func Deleting() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonDeleting,
	}
}

// Available returns a condition that indicates the resource is
// currently observed to be available for use.
func Available() metav1.Condition {
	return metav1.Condition{
		Type:               TypeReady,
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             ReasonAvailable,
	}
}

func Upsert(conds *[]metav1.Condition, co metav1.Condition) {
	for idx, el := range *conds {
		if el.Type == co.Type {
			(*conds)[idx] = co
			return
		}
	}
	*conds = append(*conds, co)
}

func Join(conds *[]metav1.Condition, all []metav1.Condition) {
	for _, el := range *conds {
		Upsert(conds, el)
	}
}

func Remove(conds *[]metav1.Condition, typ string) {
	for idx, el := range *conds {
		if el.Type == typ {
			*conds = append((*conds)[:idx], (*conds)[idx+1:]...)
			return
		}
	}
}

/*
type Status struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
*/
