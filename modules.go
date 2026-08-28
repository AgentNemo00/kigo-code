package kigocode

import (
	"context"

	"github.com/AgentNemo00/kigo-core/information"
	"github.com/AgentNemo00/kigo-core/inquiry"
	"github.com/AgentNemo00/kigo-core/notification"
	"github.com/AgentNemo00/kigo-core/order"
	"github.com/AgentNemo00/kigo-core/util"
	"github.com/AgentNemo00/sca-instruments/log"
	"github.com/AgentNemo00/sca-instruments/pubsub"
	"github.com/AgentNemo00/sca-instruments/pubsub/nats"
)

type ModuleConfig struct {
	NameTo 			string
	Name 			string
	PubSubKiGo		string
	PubSubUrl  		string
}

func GetModule(ctx context.Context, cfg *ModuleConfig) *information.ModuleInformation {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
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

	errChan := make(chan error)
	receivedChan := make(chan information.ModulesPayload)

	subscb, err := sub.Subscribe(ctx, cfg.Name, func(ctx context.Context, metadata pubsub.Metadata, data *order.Order) {
		if metadata.Error != nil {
			errChan <- err
			return
		}
		var payload order.OrderInformationPayload
		err := util.MapToStruct(data.Payload, &payload)
		if err != nil {
			errChan <- err
			return 
		}
		var p information.ModulesPayload
		err = util.MapToStruct(payload.Payload, &p)
		if err != nil {
			errChan <- err
			return 
		}
		receivedChan <- p
	})
	if err != nil {
		log.Ctx(ctx).Err(err)
		return nil
	}
	defer subscb.Unsubscribe(ctx)
	defer close(receivedChan)
	defer close(errChan)
	
	go pub.Publish(ctx, cfg.PubSubKiGo, notification.Notification{
		From: cfg.Name,
		To: cfg.PubSubKiGo,
		Notification: inquiry.InquiryInformation,
		Payload: inquiry.InquiryInformationPayload{
			Type: information.Modules,
		},
	})

	var modules information.ModulesPayload
	
	l : for {
		select{
		case <- ctx.Done():
			log.Ctx(ctx).Err(ctx.Err())
			return nil
		case ele, ok := <- receivedChan:
			if !ok {
				return nil
			}
			modules = ele
			break l
		default:
		}
	}

	var module *information.ModuleInformation

	for _, mod := range modules.Modules {
		if mod.Name == cfg.NameTo {
			module = &mod
			break
		}
	}
	
	return module
}