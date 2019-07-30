package drivers

import (
	"encoding/json"
	"log"

	producerwrapper "Work/Producer"

	"github.com/streadway/amqp"
)

//RabbitProducer - rabbit producer type holds the channel and the connection required.
type RabbitProducer struct {
	ch  *amqp.Channel
	con *amqp.Connection
}

//GetNewRabbitProducer receives a url and connect to the producer - currently Rabbit should be able to connect kafka aswell.
func GetNewRabbitProducer(url string) (producerwrapper.Producer, error) {
	ch, con, err := connectToRabbitMQ(url)
	if err != nil {
		return &RabbitProducer{}, err
	}
	return &RabbitProducer{ch: ch, con: con}, nil
}

//SendMessage - sends the message on the producer receives message and channel name
func (p *RabbitProducer) SendMessage(message interface{}, topic string) error {
	q, err := p.ch.QueueDeclare(
		topic, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments

	)
	if err != nil {
		return failOnError(err, "Failed to declare a queue")
	}

	body, err := json.Marshal(message)
	if err != nil {
		return failOnError(err, "Failed to serialize message to byte array")
	}

	err = p.ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	if err != nil {
		return failOnError(err, "Failed to publish a message")
	}
	return nil
}

func connectToRabbitMQ(url string) (*amqp.Channel, *amqp.Connection, error) {
	//amqp://guest:guest@localhost:5672/
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, failOnError(err, "Failed to connect to RabbitMQ")
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, failOnError(err, "Failed to open a channel")
	}
	return ch, conn, err
}

func failOnError(err error, msg string) error {
	log.Printf("%s: %s", msg, err)
	return err
}
