package kigocode

import (
	"context"
	"slices"

	"github.com/AgentNemo00/kigo-core/order"
	"github.com/AgentNemo00/kigo-core/util"
	"github.com/AgentNemo00/sca-instruments/log"
	ps "github.com/AgentNemo00/sca-instruments/pubsub"
	"github.com/AgentNemo00/sca-instruments/pubsub/nats"
)

type ChangesConfig struct {
	UUID 		string
	PubSubUrl  	string
	Changes 	[]string
}

func ListenForChanges(ctx context.Context, cfg *ChangesConfig, onChange func(change string, value any)) (error, func()) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sub, err := nats.SubscriberWithURL[order.Order](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return err, func ()  {}
	}

	subscription, err := sub.Subscribe(ctx, cfg.UUID, func(ctx context.Context, metadata ps.Metadata, data *order.Order)  {
		if metadata.Error != nil {
			log.Ctx(ctx).Err(err)
			return
		}
		if data.Order != order.OrderChange {
			return 
		}
		var payload order.OrderChangePayload
		err := util.MapToStruct(data.Payload, &payload)
		if err != nil {
			log.Ctx(ctx).Err(err)
			return 
		}
		if !slices.Contains(cfg.Changes, payload.Type) {
			return
		}
		onChange(payload.Type, payload.Payload)
	})
	if err != nil {
		log.Ctx(ctx).Err(err)
		return err, func ()  {}
	}
	return nil, func ()  {
		subscription.Unsubscribe(ctx)
	}
}