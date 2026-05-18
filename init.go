package contrib

import (
	"fmt"
	"time"
	"context"

	"github.com/AgentNemo00/kigo-core/notification"
	"github.com/AgentNemo00/kigo-core/order"
	"github.com/AgentNemo00/kigo-core/util"
	"github.com/AgentNemo00/sca-instruments/log"
	ps "github.com/AgentNemo00/sca-instruments/pubsub"
	"github.com/AgentNemo00/sca-instruments/pubsub/nats"
	"github.com/AgentNemo00/sca-instruments/security"
	"github.com/AgentNemo00/sca-instruments/configuration"
)

type InitConfig struct {
	Name 			string
	PubSubKiGo		string
	PubSubUrl  		string
	Changes 		[]string
	Heartbeat 		time.Duration
}

func (c *InitConfig) Default() {
	if c.PubSubUrl == "" {
		c.PubSubUrl = "nats://127.0.0.1:4222"
	}
	if c.Name == "" {
		c.Name = "unknown"
	}
	if c.PubSubKiGo == "" {
		c.PubSubKiGo = "KiGo"
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = time.Hour * 24
	}
}


func InitializeModule(ctx context.Context, start time.Time, cfg *InitConfig, onShutdown func(order.OrderShutdownPayload)) *order.OrderStartUpPayload {
	ctx, cancel := context.WithCancel(ctx)
	err := configuration.ByEnv(cfg)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}
	// pubsub publisher
	pub, err := nats.PublisherWithURL[notification.Notification](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}
	// pubsub suscriber
	sub, err := nats.SubscriberWithURL[order.Order](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}
	// generate uuid to be able to get startup order
	tempUUID, err := security.UUID()
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}

	// create channel for error and data
	errChan := make(chan error)
	dataChan := make(chan order.OrderStartUpPayload)

		// subscribe to temporary uuid to be able go get OrderStartUp
	subscription, err := sub.Subscribe(ctx, tempUUID, func(ctx context.Context, metadata ps.Metadata, data *order.Order) {
		if metadata.Error != nil {
			errChan <- err
			return
		}
		switch (data.Order) {
			case order.OrderStartUp:
				// convert
				var payload order.OrderStartUpPayload
				err := util.MapToStruct(data.Payload, &payload)
				if err != nil {
					errChan <- err
					return 
				}
				dataChan <- payload
			case order.OrderShutdown:
				var payload order.OrderShutdownPayload
				err := util.MapToStruct(data.Payload, &payload)
				if err != nil {
					errChan <- err
					return 
				}
				onShutdown(payload)
			default:
				errChan <- fmt.Errorf("wrong order: %s", data.Order)
		}
	})
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}
	defer subscription.Unsubscribe(ctx)

	go func ()  {
		errValue, ok := <-errChan
		if ok {
			log.Ctx(ctx).Err(errValue)
			close(dataChan)
			close(errChan)
			cancel()
		}
	}()

	// singalize ready
	go func ()  {
		err := pub.Publish(ctx, cfg.PubSubKiGo, notification.Notification{
			From: tempUUID,
			To: cfg.PubSubKiGo,
			Notification: notification.NotificationReady,
			Payload: notification.NotificationReadyPayload{
				Name: cfg.Name,
				Changes: cfg.Changes,
				Heartbeat: cfg.Heartbeat,                   // configure hearbeat to be needed every two minutes
				Duration: time.Now().Sub(start),			// duration needed to start up, should help to calculate hearbeat
			},
		})		
		if err != nil {
			errChan <- err
		}
	}()

	log.Ctx(ctx).Info("waiting for startup routine")
	
	value ,err := util.WaitingWithContext(ctx, dataChan)
	if err != nil {
		return nil
	}
	return value
}
