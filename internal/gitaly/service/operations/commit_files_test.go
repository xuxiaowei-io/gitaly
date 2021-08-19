	"context"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	t.Parallel()
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testUserCommitFiles)
}

func testUserCommitFiles(t *testing.T, ctx context.Context) {
	startRepo, startRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
			defer func() { require.NoError(t, os.RemoveAll(repoPath)) }()
					gittest.TestUser,
					"",
				testassert.GrpcEqualErr(t, step.error, err)

				authorDate := gittest.Exec(t, cfg, "-C", repoPath, "log", "--pretty='format:%ai'", "-1")
				require.Contains(t, string(authorDate), gittest.TimezoneOffset)
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testUserCommitFilesStableCommitID)
}
func testUserCommitFilesStableCommitID(t *testing.T, ctx context.Context) {
	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	headerRequest := headerRequest(repoProto, gittest.TestUser, "master", []byte("commit message"), "")
	setTimestamp(headerRequest, time.Unix(12345, 0))
	require.Equal(t, resp.BranchUpdate.CommitId, "23ec4ccd7fcc6ecf39431805bbff1cbcb6c23b9d")
		Id:       "23ec4ccd7fcc6ecf39431805bbff1cbcb6c23b9d",
			Date:     &timestamppb.Timestamp{Seconds: 12345},
			Timezone: []byte(gittest.TimezoneOffset),
			Name:     gittest.TestUser.Name,
			Email:    gittest.TestUser.Email,
			Date:     &timestamppb.Timestamp{Seconds: 12345},
			Timezone: []byte(gittest.TimezoneOffset),
func TestUserCommitFilesQuarantine(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testUserCommitFilesQuarantine)
}

func testUserCommitFilesQuarantine(t *testing.T, ctx context.Context) {
	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

	// Set up a hook that parses the new object and then aborts the update. Like this, we can
	// assert that the object does not end up in the main repository.
	hookScript := fmt.Sprintf("#!/bin/sh\n%s rev-parse $3^{commit} && exit 1", cfg.Git.BinPath)
	gittest.WriteCustomHook(t, repoPath, "update", []byte(hookScript))

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)

	headerRequest := headerRequest(repoProto, gittest.TestUser, "master", []byte("commit message"), "")
	setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))
	setTimestamp(headerRequest, time.Unix(12345, 0))
	require.NoError(t, stream.Send(headerRequest))

	require.NoError(t, stream.Send(createFileHeaderRequest("file.txt")))
	require.NoError(t, stream.Send(actionContentRequest("content")))
	response, err := stream.CloseAndRecv()
	require.NoError(t, err)

	oid, err := git.NewObjectIDFromHex(strings.TrimSpace(response.PreReceiveError))
	require.NoError(t, err)
	exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
	require.NoError(t, err)

	// The new commit will be in the target repository in case quarantines are disabled.
	// Otherwise, it should've been discarded.
	require.Equal(t, !featureflag.Quarantine.IsEnabled(ctx), exists)
}

	t.Parallel()
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testSuccessfulUserCommitFilesRequest)
}

func testSuccessfulUserCommitFilesRequest(t *testing.T, ctx context.Context) {
	newRepo, newRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		startBranchName string
		{
			desc:            "existing repo, new branch, with start branch",
			repo:            repo,
			repoPath:        repoPath,
			branchName:      "new-branch-with-start-branch",
			startBranchName: "master",
			repoCreated:     false,
			branchCreated:   true,
		},
			headerRequest := headerRequest(tc.repo, gittest.TestUser, tc.branchName, commitFilesMessage, tc.startBranchName)
			require.Equal(t, gittest.TestUser.Name, headCommit.Committer.Name)
			require.Equal(t, gittest.TestUser.Email, headCommit.Committer.Email)
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testSuccessfulUserCommitFilesRequestMove)
}
func testSuccessfulUserCommitFilesRequestMove(t *testing.T, ctx context.Context) {
			testRepo, testRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
			headerRequest := headerRequest(testRepo, gittest.TestUser, branchName, commitFilesMessage, "")
	t.Parallel()
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testSuccessfulUserCommitFilesRequestForceCommit)
}

func testSuccessfulUserCommitFilesRequestForceCommit(t *testing.T, ctx context.Context) {
	headerRequest := headerRequest(repoProto, gittest.TestUser, targetBranchName, commitFilesMessage, "")
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testSuccessfulUserCommitFilesRequestStartSha)
}
func testSuccessfulUserCommitFilesRequestStartSha(t *testing.T, ctx context.Context) {
	headerRequest := headerRequest(repoProto, gittest.TestUser, targetBranchName, commitFilesMessage, "")
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
	}))
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
	}))
func testSuccessfulUserCommitFilesRemoteRepositoryRequest(setHeader func(header *gitalypb.UserCommitFilesRequest)) func(*testing.T, context.Context) {
	return func(t *testing.T, ctx context.Context) {
		newRepoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
		headerRequest := headerRequest(newRepoProto, gittest.TestUser, targetBranchName, commitFilesMessage, "")
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature)
}
func testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature(t *testing.T, ctx context.Context) {
	repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
			user:   &gitalypb.User{Name: []byte(".,:;<>\"'\nJane Doe.,:;<>'\"\n"), Email: []byte(".,:;<>'\"\njanedoe@gitlab.com.,:;<>'\"\n"), GlId: gittest.GlID},
			user:   &gitalypb.User{Name: []byte("Ja<ne\n D>oe"), Email: []byte("ja<ne\ndoe>@gitlab.com"), GlId: gittest.GlID},
			headerRequest := headerRequest(repoProto, tc.user, targetBranchName, commitFilesMessage, "")
	t.Parallel()
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testFailedUserCommitFilesRequestDueToHooks)
}

func testFailedUserCommitFilesRequestDueToHooks(t *testing.T, ctx context.Context) {
	headerRequest := headerRequest(repoProto, gittest.TestUser, branchName, commitFilesMessage, "")
			require.Contains(t, resp.PreReceiveError, "GL_ID="+gittest.TestUser.GlId)
			require.Contains(t, resp.PreReceiveError, "GL_USERNAME="+gittest.TestUser.GlUsername)
	t.Parallel()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testFailedUserCommitFilesRequestDueToIndexError)
}
func testFailedUserCommitFilesRequestDueToIndexError(t *testing.T, ctx context.Context) {
				headerRequest(repo, gittest.TestUser, "feature", commitFilesMessage, ""),
				headerRequest(repo, gittest.TestUser, "feature", commitFilesMessage, ""),
				headerRequest(repo, gittest.TestUser, "utf-dir", commitFilesMessage, ""),
	t.Parallel()
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.Quarantine,
	}).Run(t, testFailedUserCommitFilesRequest)
}

func testFailedUserCommitFilesRequest(t *testing.T, ctx context.Context) {
			req:  headerRequest(nil, gittest.TestUser, branchName, commitFilesMessage, ""),
			req:  headerRequest(repo, nil, branchName, commitFilesMessage, ""),
			req:  headerRequest(repo, gittest.TestUser, "", commitFilesMessage, ""),
			req:  headerRequest(repo, gittest.TestUser, branchName, nil, ""),
			req:  setStartSha(headerRequest(repo, gittest.TestUser, branchName, commitFilesMessage, ""), "foobar"),
			req:  headerRequest(repo, &gitalypb.User{}, branchName, commitFilesMessage, ""),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte(""), Email: []byte("")}, branchName, commitFilesMessage, ""),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte(" "), Email: []byte(" ")}, branchName, commitFilesMessage, ""),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte("Jane Doe"), Email: []byte("")}, branchName, commitFilesMessage, ""),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte(""), Email: []byte("janedoe@gitlab.com")}, branchName, commitFilesMessage, ""),
func headerRequest(repo *gitalypb.Repository, user *gitalypb.User, branchName string, commitMessage []byte, startBranchName string) *gitalypb.UserCommitFilesRequest {
				StartBranchName: []byte(startBranchName),
func setTimestamp(headerRequest *gitalypb.UserCommitFilesRequest, time time.Time) {
	getHeader(headerRequest).Timestamp = timestamppb.New(time)