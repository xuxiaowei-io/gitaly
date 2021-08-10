	"github.com/golang/protobuf/ptypes"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
					testhelper.TestUser,
				require.Equal(t, step.error, err)
	headerRequest := headerRequest(repoProto, testhelper.TestUser, "master", []byte("commit message"))
	setTimestamp(t, headerRequest, time.Unix(12345, 0))
	require.Equal(t, resp.BranchUpdate.CommitId, "4f0ca1fbf05e04dbd5f68d14677034e0afee58ff")
		Id:       "4f0ca1fbf05e04dbd5f68d14677034e0afee58ff",
			Timezone: []byte("+0000"),
			Name:     testhelper.TestUser.Name,
			Email:    testhelper.TestUser.Email,
			Timezone: []byte("+0000"),
			headerRequest := headerRequest(tc.repo, testhelper.TestUser, tc.branchName, commitFilesMessage)
			require.Equal(t, testhelper.TestUser.Name, headCommit.Committer.Name)
			require.Equal(t, testhelper.TestUser.Email, headCommit.Committer.Email)
			testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
			headerRequest := headerRequest(testRepo, testhelper.TestUser, branchName, commitFilesMessage)
	headerRequest := headerRequest(repoProto, testhelper.TestUser, targetBranchName, commitFilesMessage)
	headerRequest := headerRequest(repoProto, testhelper.TestUser, targetBranchName, commitFilesMessage)
		headerRequest := headerRequest(newRepoProto, testhelper.TestUser, targetBranchName, commitFilesMessage)
			user:   &gitalypb.User{Name: []byte(".,:;<>\"'\nJane Doe.,:;<>'\"\n"), Email: []byte(".,:;<>'\"\njanedoe@gitlab.com.,:;<>'\"\n"), GlId: testhelper.GlID},
			user:   &gitalypb.User{Name: []byte("Ja<ne\n D>oe"), Email: []byte("ja<ne\ndoe>@gitlab.com"), GlId: testhelper.GlID},
			headerRequest := headerRequest(repoProto, tc.user, targetBranchName, commitFilesMessage)
	headerRequest := headerRequest(repoProto, testhelper.TestUser, branchName, commitFilesMessage)
			require.Contains(t, resp.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)
			require.Contains(t, resp.PreReceiveError, "GL_USERNAME="+testhelper.TestUser.GlUsername)
				headerRequest(repo, testhelper.TestUser, "feature", commitFilesMessage),
				headerRequest(repo, testhelper.TestUser, "feature", commitFilesMessage),
				headerRequest(repo, testhelper.TestUser, "utf-dir", commitFilesMessage),
			req:  headerRequest(nil, testhelper.TestUser, branchName, commitFilesMessage),
			req:  headerRequest(repo, nil, branchName, commitFilesMessage),
			req:  headerRequest(repo, testhelper.TestUser, "", commitFilesMessage),
			req:  headerRequest(repo, testhelper.TestUser, branchName, nil),
			req:  setStartSha(headerRequest(repo, testhelper.TestUser, branchName, commitFilesMessage), "foobar"),
			req:  headerRequest(repo, &gitalypb.User{}, branchName, commitFilesMessage),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte(""), Email: []byte("")}, branchName, commitFilesMessage),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte(" "), Email: []byte(" ")}, branchName, commitFilesMessage),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte("Jane Doe"), Email: []byte("")}, branchName, commitFilesMessage),
			req:  headerRequest(repo, &gitalypb.User{Name: []byte(""), Email: []byte("janedoe@gitlab.com")}, branchName, commitFilesMessage),
func headerRequest(repo *gitalypb.Repository, user *gitalypb.User, branchName string, commitMessage []byte) *gitalypb.UserCommitFilesRequest {
				StartBranchName: nil,
func setTimestamp(t testing.TB, headerRequest *gitalypb.UserCommitFilesRequest, time time.Time) {
	timestamp, err := ptypes.TimestampProto(time)
	require.NoError(t, err)
	getHeader(headerRequest).Timestamp = timestamp