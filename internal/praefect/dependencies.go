package praefect

import (
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v16/internal/grpc/proxy"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/v16/internal/praefect/transactions"
)

// Dependencies consolidates Praefect service dependencies for injection.
type Dependencies struct {
	Config          config.Config
	Logger          logrus.FieldLogger
	Coordinator     *Coordinator
	Director        proxy.StreamDirector
	NodeMgr         nodes.Manager
	TxMgr           *transactions.Manager
	Queue           datastore.ReplicationEventQueue
	RepositoryStore datastore.RepositoryStore
	AssignmentStore AssignmentStore
	Router          Router
	Registry        *protoregistry.Registry
	Conns           Connections
	PrimaryGetter   PrimaryGetter
	Checks          []service.CheckFunc
}
