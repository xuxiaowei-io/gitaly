package repositoryService_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRepositoryService(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "RepositoryService Suite")
}
