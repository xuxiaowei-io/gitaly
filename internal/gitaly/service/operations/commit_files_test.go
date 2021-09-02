	"github.com/golang/protobuf/ptypes/timestamp"
var (
	commitFilesMessage = []byte("Change files")
)
	startRepo, startRepoPath, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
	t.Cleanup(cleanup)
			defer os.RemoveAll(repoPath)
	repoProto, repoPath, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
	defer cleanup()
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Date:     &timestamp.Timestamp{Seconds: 12345},
	newRepo, newRepoPath, newRepoCleanupFn := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
	defer newRepoCleanupFn()
			testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
			defer cleanupFn()
func testSuccessfulUserCommitFilesRemoteRepositoryRequest(setHeader func(header *gitalypb.UserCommitFilesRequest)) func(*testing.T) {
	return func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		newRepoProto, _, newRepoCleanupFn := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
		defer newRepoCleanupFn()
	repoProto, _, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
	defer cleanup()