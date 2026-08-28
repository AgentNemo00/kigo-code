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

func ListenForChanges(ctx context.Context, cfg *ChangesConfig, onChange func(change string, value any)) (func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sub, err := nats.SubscriberWithURL[order.Order](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return func ()  {}, err
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
		return func ()  {}, err
	}
	return func ()  {
		subscription.Unsubscribe(ctx)
	}, nil
}

type ChangeConfig struct {
	NameTo 		string
	Name 		string
	UUID 		string
	PubSubUrl  	string
	PubSubKiGo	string
	Change 		string
	Value 		any
}

func SendChange(ctx context.Context, cfg *ChangeConfig) bool {
	module := GetModule(ctx, &ModuleConfig{
		NameTo: cfg.NameTo,
		Name: cfg.Name,
		PubSubUrl: cfg.PubSubUrl,
		PubSubKiGo: cfg.PubSubKiGo,
	})
	if module == nil {
		return false
	}
	if !slices.Contains(module.Changes, cfg.Change) {
		log.Ctx(ctx).Error("Change %s is not configured for %s for module %s", cfg.Change, module.Changes, cfg.Name)
		return false
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pub, err := nats.PublisherWithURL[order.Order](cfg.PubSubUrl)
	if err != nil {
		log.Ctx(ctx).Err(err)
		return false
	}
	err = pub.Publish(ctx, module.ID, order.Order{
		From: cfg.UUID,
		To: module.ID,
		Order: order.OrderChange,
		Payload: order.OrderChangePayload{
			Type: cfg.Change,
			Payload: cfg.Value,
		},
	})
	if err != nil {
		log.Ctx(ctx).Err(err)
		return false
	}
	return true
}
