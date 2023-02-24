package trace2

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/correlation"
	"os"
)

type TraceHandler func(context.Context, *Trace)

type Hook interface {
	Activate(ctx context.Context, path string, args []string) (TraceHandler, bool)
}

func NewManager(ctx context.Context, hooks []Hook, path string, args []string) *Manager {
	manager := &Manager{
		ctx:    ctx,
		logger: ctxlogrus.Extract(ctx),
	}
	for _, hook := range hooks {
		if handler, ok := hook.Activate(ctx, path, args); ok {
			manager.handlers = append(manager.handlers, handler)
		}
	}
	return manager
}

type Manager struct {
	ctx      context.Context
	logger   *logrus.Entry
	handlers []TraceHandler
	fd       *os.File
}

func (m *Manager) Inject(env []string) ([]string, error) {
	if len(m.handlers) <= 0 {
		return env, nil
	}

	fd, err := os.CreateTemp("", "gitaly-trace2")
	if err != nil {
		return env, fmt.Errorf("trace2 create tempfile: %w", err)
	}
	m.fd = fd

	env = append(
		env,
		"GIT_TRACE2_BRIEF=true",
		fmt.Sprintf("GIT_TRACE2_EVENT=%s", fd.Name()),
		fmt.Sprintf("GIT_TRACE2_PARENT_SID=%s", correlation.ExtractFromContextOrGenerate(m.ctx)),
	)
	return env, nil
}

func (m *Manager) Finish() {
	if len(m.handlers) <= 0 {
		return
	}

	defer func() {
		err := m.fd.Close()
		if err != nil {
			m.logger.Errorf("trace2: fail to close tempfile: %s", err)
		}
		err = os.Remove(m.fd.Name())
		if err != nil {
			m.logger.Errorf("trace2: fail to remove tempfile: %s", err)
		}
	}()

	parser := &Parser{}
	trace, err := parser.Parse(m.ctx, m.fd)
	if err != nil {
		m.logger.Errorf("trace2: fail to parse events: %s", err)
		return
	}

	for _, handler := range m.handlers {
		handler(m.ctx, trace)
	}
}
