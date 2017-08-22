package nsqtest

import (
	"fmt"

	"github.com/bitly/go-nsq"
	"gopkg.in/ory-am/dockertest.v3"
)

type Server struct {}

func NewServer() *Server {
	return &Server{}
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

	resource := new(dockertest.Resource)

	if err := pool.Retry(func() error {
		var err error
		resource, err = pool.RunWithOptions(options)

		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &Nsq{
		Host: fmt.Sprintf("localhost:%s", resource.GetPort("4150/tcp")),

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
