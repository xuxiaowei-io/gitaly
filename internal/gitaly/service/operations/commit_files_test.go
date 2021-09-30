	"context"
var commitFilesMessage = []byte("Change files")

	startRepo, startRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
			defer func() { require.NoError(t, os.RemoveAll(repoPath)) }()

	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
			Date:     &timestamppb.Timestamp{Seconds: 12345},
			Date:     &timestamppb.Timestamp{Seconds: 12345},
func TestUserCommitFilesQuarantine(t *testing.T) {
	t.Parallel()

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

	outputPath := filepath.Join(testhelper.TempDir(t), "output")

	// Set up a hook that parses the new object and then aborts the update. Like this, we can
	// assert that the object does not end up in the main repository.
	hookScript := fmt.Sprintf("#!/bin/sh\nread oldval newval ref && %s rev-parse $newval^{commit} >%s && exit 1", cfg.Git.BinPath, outputPath)
	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(hookScript))

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)

	headerRequest := headerRequest(repoProto, gittest.TestUser, "master", []byte("commit message"), "")
	setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))
	setTimestamp(headerRequest, time.Unix(12345, 0))
	require.NoError(t, stream.Send(headerRequest))

	require.NoError(t, stream.Send(createFileHeaderRequest("file.txt")))
	require.NoError(t, stream.Send(actionContentRequest("content")))
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	hookOutput := testhelper.MustReadFile(t, outputPath)
	oid, err := git.NewObjectIDFromHex(text.ChompBytes(hookOutput))
	require.NoError(t, err)
	exists, err := repo.HasRevision(ctx, oid.Revision()+"^{commit}")
	require.NoError(t, err)

	require.False(t, exists, "quarantined commit should have been discarded")
}


	newRepo, newRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

			testRepo, testRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])




func testSuccessfulUserCommitFilesRemoteRepositoryRequest(setHeader func(header *gitalypb.UserCommitFilesRequest)) func(*testing.T, context.Context) {
	return func(t *testing.T, ctx context.Context) {
		newRepoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])

	repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])


