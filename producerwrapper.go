package producerwrapper

//Producer Interface is a interface that every producer driver need to complete in order to use the wrapper
type Producer interface {
	SendMessage(parameters interface{}, topic string) error
}
