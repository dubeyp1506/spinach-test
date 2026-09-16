// Package wire holds the integration adapters that connect module-defined
// consumer interfaces to their concrete implementations. Owned by integration
// (A0) — modules never import each other.
package wire

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/spinach/martech-engine/internal/ai"
	"github.com/spinach/martech-engine/internal/campaigns"
	"github.com/spinach/martech-engine/internal/customers"
	"github.com/spinach/martech-engine/internal/events"
)

// NewProcessor adapts customers.Applier to events.Processor (CONTRACTS §2).
// The worker owns the transaction; the applier only mutates within it.
func NewProcessor() events.Processor {
	return &processorAdapter{a: customers.NewApplier()}
}

type processorAdapter struct{ a *customers.Applier }

func (p *processorAdapter) ProcessTx(ctx context.Context, tx pgx.Tx, evt events.StoredEvent) error {
	return p.a.ProcessTx(ctx, tx, customers.ApplierEvent{
		EventDBID:  evt.ID,
		EventID:    evt.EventID,
		CustomerID: evt.CustomerID,
		CampaignID: evt.CampaignID,
		Channel:    evt.Channel,
		Type:       evt.Type,
		OccurredAt: evt.OccurredAt,
		Payload:    evt.Payload,
	})
}

// NewMetricsProvider adapts campaigns.Service to ai.MetricsProvider
// (CONTRACTS §3) — the AI layer never imports internal/campaigns.
func NewMetricsProvider(s *campaigns.Service) ai.MetricsProvider {
	return &metricsAdapter{s: s}
}

type metricsAdapter struct{ s *campaigns.Service }

func (m *metricsAdapter) Metrics(ctx context.Context, campaignID int64) (*ai.Metrics, error) {
	cm, err := m.s.Metrics(ctx, campaignID)
	if err != nil {
		return nil, err
	}
	out := &ai.Metrics{
		CampaignID:     cm.CampaignID,
		Sends:          cm.Sends,
		Delivered:      cm.Delivered,
		Opens:          cm.Opens,
		Clicks:         cm.Clicks,
		Conversions:    cm.Conversions,
		Bounces:        cm.Bounces,
		Unsubscribes:   cm.Unsubscribes,
		OpenRate:       cm.OpenRate,
		ClickRate:      cm.ClickRate,
		ConversionRate: cm.ConversionRate,
		FirstEventAt:   cm.FirstEventAt,
		LastEventAt:    cm.LastEventAt,
		ByChannel:      make(map[string]ai.ChannelMetrics, len(cm.ByChannel)),
	}
	for ch, c := range cm.ByChannel {
		out.ByChannel[ch] = ai.ChannelMetrics{
			Sends: c.Sends, Delivered: c.Delivered, Opens: c.Opens,
			Clicks: c.Clicks, Conversions: c.Conversions, Bounces: c.Bounces,
			Unsubscribes: c.Unsubscribes, OpenRate: c.OpenRate,
			ClickRate: c.ClickRate, ConversionRate: c.ConversionRate,
		}
	}
	return out, nil
}
