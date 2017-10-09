package nsqtest

import (
	"fmt"
	"time"

	"github.com/bitly/go-nsq"
	"gopkg.in/ory-am/dockertest.v3"
)

type Server struct {
	Max time.Duration
}

func NewServer(maxBackoffInterval time.Duration) *Server {
	return &Server{Max: maxBackoffInterval}
}

func (s *Server) Run() (*Nsq, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	options := &dockertest.RunOptions{
		Repository: "nsqio/nsq",
		Tag:        "latest",
		Cmd:        []string{"nsqd"},
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		return nil, err
	}

	host := fmt.Sprintf("localhost:%s", resource.GetPort("4150/tcp"))

	attempt := 1

	for {
		consumer, _ := nsq.NewConsumer("test", "ch", nsq.NewConfig())
		consumer.AddHandler(new(fakeNSQHandler))
		err := consumer.ConnectToNSQD(host)

		if err != nil {
			duration := time.Millisecond * time.Duration(attempt)

			if duration > s.Max {
				time.Sleep(s.Max)
				continue
			}

			time.Sleep(duration)
			attempt++
			continue
		}

		consumer.Stop()
		break
	}

	return &Nsq{
		Host: host,

		pool:     pool,
		resource: resource,
	}, nil
}

type Nsq struct {
	Host string

	pool     *dockertest.Pool
	resource *dockertest.Resource
}

func (n *Nsq) NewProducer(config *nsq.Config) (*nsq.Producer, error) {
	p, err := nsq.NewProducer(n.Host, config)
	return p, err
}

func (n *Nsq) NewConsumer(topic string, channel string, config *nsq.Config) (*nsq.Consumer, error) {
	c, err := nsq.NewConsumer(topic, channel, config)
	return c, err
}

func (s *Nsq) Purge() error {
	err := s.pool.Purge(s.resource)
	return err
}

type fakeNSQHandler struct{}

func (h *fakeNSQHandler) HandleMessage(message *nsq.Message) error {
	return nil
}
