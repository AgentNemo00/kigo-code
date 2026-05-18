package kigocode

import (
	"context"
	"fmt"
	"github.com/AgentNemo00/kigo-core/information"
	"github.com/AgentNemo00/sca-instruments/pubsub/nats"
	"github.com/AgentNemo00/kigo-core/notification"
	"github.com/AgentNemo00/sca-instruments/log"
	"github.com/AgentNemo00/kigo-core/inquiry"
	"github.com/AgentNemo00/kigo-core/order"
	"github.com/AgentNemo00/kigo-core/util"
	ps "github.com/AgentNemo00/sca-instruments/pubsub"
)

type UIConfig struct {
	PubSubKiGoUI	string
	PubSubUrl  		string
	UUID 				string
}

func GetUIInformation(ctx context.Context, cfg *UIConfig) (*information.UIPayload, *information.ScreenPayload) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pub, err := nats.PublisherWithURL[notification.Notification](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil, nil
	}
	// pubsub suscriber
	sub, err := nats.SubscriberWithURL[order.Order](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil, nil
	}

	errChan := make(chan error)
	chanUI := make(chan information.UIPayload)
	chanScreen := make(chan information.ScreenPayload)
	subscription, err := sub.Subscribe(ctx, cfg.UUID, func(ctx context.Context, metadata ps.Metadata, data *order.Order)  {
		if metadata.Error != nil {
			log.Ctx(ctx).Err(err)
			return
		}
		switch data.Order {
			case order.OrderInformation:
				var payloadUI information.UIPayload
				err := util.MapToStruct(data.Payload, &payloadUI)
				if err == nil && len(payloadUI.Channels) != 0 {
					log.Ctx(ctx).Debug("got ui information: %#v", payloadUI)
					chanUI <- payloadUI
					return
				} 
				var payloadScreen information.ScreenPayload
				err = util.MapToStruct(data.Payload, &payloadScreen)
				if err == nil {
					log.Ctx(ctx).Debug("got screen information: %#v", payloadScreen)
					chanScreen <- payloadScreen
					return
				}
			default:
				errChan <- fmt.Errorf("wrong order: %s, %v", data.Order, data.Payload)
		}
	})
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil, nil
	}
	defer subscription.Unsubscribe(ctx)

	go func ()  {
		errValue, ok := <-errChan
		if ok {
			log.Ctx(ctx).Err(errValue)
			close(chanUI)
			close(chanScreen)
			close(errChan)
			cancel()
		}
	}()


	go func ()  {
		log.Ctx(ctx).Info("get ui information")
		err = pub.Publish(ctx, cfg.PubSubKiGoUI, notification.Notification{
			From: cfg.UUID,
			To: cfg.PubSubKiGoUI,
			Notification: inquiry.InquiryInformation,
			Payload: inquiry.InquiryInformationPayload{
				Type: information.UI,
			},
		})
		if err != nil {
			log.Ctx(ctx).Err(err)
			return
		}		
	}()

	go func ()  {
		log.Ctx(ctx).Info("get screen information")
		err = pub.Publish(ctx, cfg.PubSubKiGoUI, notification.Notification{
			From: cfg.UUID,
			To: cfg.PubSubKiGoUI,
			Notification: inquiry.InquiryInformation,
			Payload: inquiry.InquiryInformationPayload{
				Type: information.Screen,
			},
		})
		if err != nil {
			log.Ctx(ctx).Err(err)
			return
		}		
	}()

	var dataUI *information.UIPayload
	var dataScreen *information.ScreenPayload

	for {
		select {
			case <- ctx.Done():
				return nil, nil
			case data := <- chanUI:
				dataUI = &data
			case data := <- chanScreen:
				dataScreen = &data
		}
		if dataUI != nil && dataScreen != nil {
			return dataUI, dataScreen
		}
	}
}