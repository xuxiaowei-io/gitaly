package log

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type reporter struct {
	interceptors.CallMeta

	ctx             context.Context
	kind            string
	startCallLogged bool

	opts   *options
	fields logging.Fields
	logger logging.Logger
}

func (c *reporter) PostCall(err error, duration time.Duration) {
	if !has(c.opts.loggableEvents, logging.FinishCall) {
		return
	}
	if err == io.EOF {
		err = nil
	}

	code := c.opts.codeFunc(err)
	fields := c.fields.WithUnique(logging.ExtractFields(c.ctx))
	fields = fields.AppendUnique(logging.Fields{"grpc.code", code.String()})
	if err != nil {
		fields = fields.AppendUnique(logging.Fields{"grpc.error", fmt.Sprintf("%v", err)})
	}

	// Appending fields from fields producers, this is our customer logic versus
	// what is defined originally in the go-grpc-middleware v2 reporter struct in logging package.
	for _, fieldsProducer := range c.opts.filedProducers {
		for key, val := range fieldsProducer(c.ctx, err) {
			fields.Delete(key)
			fields = fields.AppendUnique(logging.Fields{key, val})
		}
	}

	msg := fmt.Sprintf("finished %s call with code %s", c.CallMeta.Typ, code.String())

	c.logger.Log(c.ctx, c.opts.levelFunc(code), msg, fields.AppendUnique(c.opts.durationFieldFunc(duration))...)
}

func (c *reporter) PostMsgSend(payload any, err error, duration time.Duration) {
	logLvl := c.opts.levelFunc(c.opts.codeFunc(err))
	fields := c.fields.WithUnique(logging.ExtractFields(c.ctx))
	if err != nil {
		fields = fields.AppendUnique(logging.Fields{"grpc.error", fmt.Sprintf("%v", err)})
	}
	if !c.startCallLogged && has(c.opts.loggableEvents, logging.StartCall) {
		c.startCallLogged = true
		c.logger.Log(c.ctx, logLvl, "started call", fields.AppendUnique(c.opts.durationFieldFunc(duration))...)
	}

	if err != nil || !has(c.opts.loggableEvents, logging.PayloadSent) {
		return
	}
	if c.CallMeta.IsClient {
		p, ok := payload.(proto.Message)
		if !ok {
			c.logger.Log(
				c.ctx,
				logging.LevelError,
				"payload is not a google.golang.org/protobuf/proto.Message; programmatic error?",
				fields.AppendUnique(logging.Fields{"grpc.request.type", fmt.Sprintf("%T", payload)})...,
			)
			return
		}

		fields = fields.AppendUnique(logging.Fields{"grpc.send.duration", duration.String(), "grpc.request.content", p})
		c.logger.Log(c.ctx, logLvl, "request sent", fields...)
	} else {
		p, ok := payload.(proto.Message)
		if !ok {
			c.logger.Log(
				c.ctx,
				logging.LevelError,
				"payload is not a google.golang.org/protobuf/proto.Message; programmatic error?",
				fields.AppendUnique(logging.Fields{"grpc.response.type", fmt.Sprintf("%T", payload)})...,
			)
			return
		}

		fields = fields.AppendUnique(logging.Fields{"grpc.send.duration", duration.String(), "grpc.response.content", p})
		c.logger.Log(c.ctx, logLvl, "response sent", fields...)
	}
}

func (c *reporter) PostMsgReceive(payload any, err error, duration time.Duration) {
	logLvl := c.opts.levelFunc(c.opts.codeFunc(err))
	fields := c.fields.WithUnique(logging.ExtractFields(c.ctx))
	if err != nil {
		fields = fields.AppendUnique(logging.Fields{"grpc.error", fmt.Sprintf("%v", err)})
	}
	if !c.startCallLogged && has(c.opts.loggableEvents, logging.StartCall) {
		c.startCallLogged = true
		c.logger.Log(c.ctx, logLvl, "started call", fields.AppendUnique(c.opts.durationFieldFunc(duration))...)
	}

	if err != nil || !has(c.opts.loggableEvents, logging.PayloadReceived) {
		return
	}
	if !c.CallMeta.IsClient {
		p, ok := payload.(proto.Message)
		if !ok {
			c.logger.Log(
				c.ctx,
				logging.LevelError,
				"payload is not a google.golang.org/protobuf/proto.Message; programmatic error?",
				fields.AppendUnique(logging.Fields{"grpc.request.type", fmt.Sprintf("%T", payload)})...,
			)
			return
		}

		fields = fields.AppendUnique(logging.Fields{"grpc.recv.duration", duration.String(), "grpc.request.content", p})
		c.logger.Log(c.ctx, logLvl, "request received", fields...)
	} else {
		p, ok := payload.(proto.Message)
		if !ok {
			c.logger.Log(
				c.ctx,
				logging.LevelError,
				"payload is not a google.golang.org/protobuf/proto.Message; programmatic error?",
				fields.AppendUnique(logging.Fields{"grpc.response.type", fmt.Sprintf("%T", payload)})...,
			)
			return
		}

		fields = fields.AppendUnique(logging.Fields{"grpc.recv.duration", duration.String(), "grpc.response.content", p})
		c.logger.Log(c.ctx, logLvl, "response received", fields...)
	}
}

func reportable(logger logging.Logger, opts *options) interceptors.CommonReportableFunc {
	return func(ctx context.Context, c interceptors.CallMeta) (interceptors.Reporter, context.Context) {
		fields := logging.Fields{}
		kind := logging.KindServerFieldValue
		if c.IsClient {
			kind = logging.KindClientFieldValue
		}

		fields = fields.WithUnique(logging.ExtractFields(ctx))

		// Appending fields from fields producers
		for _, fieldsProducer := range opts.filedProducers {
			for key, val := range fieldsProducer(ctx, nil) {
				fields.AppendUnique(logging.Fields{key, val})
			}
		}

		if !c.IsClient {
			if peer, ok := peer.FromContext(ctx); ok {
				fields = append(fields, "peer.address", peer.Addr.String())
			}
		}

		singleUseFields := logging.Fields{"grpc.start_time", time.Now().Format(opts.timestampFormat)}
		if d, ok := ctx.Deadline(); ok {
			singleUseFields = singleUseFields.AppendUnique(logging.Fields{"grpc.request.deadline", d.Format(opts.timestampFormat)})
		}
		return &reporter{
			CallMeta:        c,
			ctx:             ctx,
			startCallLogged: false,
			opts:            opts,
			fields:          fields.WithUnique(singleUseFields),
			logger:          logger,
			kind:            kind,
		}, logging.InjectFields(ctx, fields)
	}
}
