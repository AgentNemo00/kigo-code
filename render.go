package contrib

import (
	"context"
	"time"

	"github.com/AgentNemo00/kigo-core/inquiry"
	"github.com/AgentNemo00/kigo-core/notification"
	"github.com/AgentNemo00/kigo-core/order"
	"github.com/AgentNemo00/kigo-core/util"
	"github.com/AgentNemo00/sca-instruments/log"
	ps "github.com/AgentNemo00/sca-instruments/pubsub"
	"github.com/AgentNemo00/sca-instruments/pubsub/nats"
)

type RenderConfig struct {
	PubSubKiGoUI	string
	PubSubUrl  		string
	ID 				string
	Channel 		string
	Format 			string
	FPS 			int
	Time 			time.Duration
	MaxFrameSize 	int
	Timeout        	time.Duration
	ObjectID 		int
}

func GetChannel(ctx context.Context, cfg *RenderConfig) *order.OrderRenderPayload {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sub, err := nats.SubscriberWithURL[order.Order](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}

	pub, err := nats.PublisherWithURL[notification.Notification](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}

	chanRenderPayload := make(chan order.OrderRenderPayload)
	subscription, err := sub.Subscribe(ctx, cfg.ID, func(ctx context.Context, metadata ps.Metadata, data *order.Order)  {
		if metadata.Error != nil {
			log.Ctx(ctx).Err(err)
			return
		}
		switch data.Order {
			case order.OrderRender:
				var payload order.OrderRenderPayload
				err := util.MapToStruct(data.Payload, &payload)
				if err != nil {
					log.Ctx(ctx).Err(err)
					return 
				}
				log.Ctx(ctx).Debug("%#v", payload)
				chanRenderPayload <- payload
		}
	})
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}
	defer subscription.Unsubscribe(ctx)

	go func ()  {
		err := pub.Publish(ctx, cfg.PubSubKiGoUI, notification.Notification{
			From: cfg.ID,
			To: cfg.PubSubKiGoUI,
			Notification: inquiry.InquiryRender,
			Payload: inquiry.InquiryRenderPayload{
				Format: cfg.Format,
				Channel: cfg.Channel,
				FPS: cfg.FPS,
				MaxFrameSize: cfg.MaxFrameSize,
				Timeout: cfg.Timeout,
				Time: cfg.Time,
				ObjectID: cfg.ObjectID,
			},
		})
		if err != nil {
			log.Ctx(ctx).Err(err)
			return
		}		
	}()
	
	for {
		select{
		case <- ctx.Done():
			return nil
		case value := <-chanRenderPayload:
			return &value
		}
	}
}
