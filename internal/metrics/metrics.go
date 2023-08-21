package metrics

const (
	// Namespace for all service metrics
	Namespace = "analytics_service"
	// ErrLabel is error static label
	ErrLabel = "error"
)

// ErrLabelValue returns string representation of error label value
func ErrLabelValue(err error) string {
	if err != nil {
		return "true"
	}
	return "false"
}
