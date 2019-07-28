package producerwrapper

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

//Producer - rabbit producer type holds the channel and the connection required.
type Producer struct {
	ch  *amqp.Channel
	con *amqp.Connection
}

//Initialize receives a url and connect to the producer - currently Rabbit should be able to connect kafka aswell.
func Initialize(url string) (*Producer, error) {
	ch, con, err := connectToRabbitMQ(url)
	if err != nil {
		return &Producer{}, err
	}
	return &Producer{ch: ch, con: con}, nil
}

//SendMessage - sends the message on the producer receives message and channel name
func (p *Producer) SendMessage(message interface{}, name string) error {
	q, err := p.ch.QueueDeclare(
		name,  // name
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
