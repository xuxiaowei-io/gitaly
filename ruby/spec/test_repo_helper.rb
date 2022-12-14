require 'fileutils'
require 'securerandom'
require 'rugged'
require 'spec_helper'

$:.unshift(File.expand_path('../proto', __dir__))
require 'gitaly'

if ENV.key?('GITALY_TESTING_GIT_BINARY')
  GIT_BINARY_PATH = ENV['GITALY_TESTING_GIT_BINARY']
elsif ENV.key?('GITALY_TESTING_BUNDLED_GIT_PATH')
  GIT_BINARY_PATH = File.join(ENV['GITALY_TESTING_BUNDLED_GIT_PATH'], 'gitaly-git-v2.38')
  GIT_EXEC_PATH = File.join(TMP_DIR, 'git-exec-path')

  # We execute git-clone(1) to set up the test repo, and this requires Git to
  # find git-upload-pack(1). We thus symlink it into a temporary Git exec path
  # and make it known to Git where it lives.
  Dir.mkdir(GIT_EXEC_PATH)
  File.symlink(GIT_BINARY_PATH, File.join(GIT_EXEC_PATH, 'git-upload-pack'))
else
  GIT_BINARY_PATH = 'git'.freeze
end

Gitlab.config.git.test_global_ivar_override(:bin_path, GIT_BINARY_PATH)
Gitlab.config.git.test_global_ivar_override(:hooks_directory, File.join(GITALY_RUBY_DIR, "hooks"))
Gitlab.config.gitaly.test_global_ivar_override(:bin_dir, __dir__)

DEFAULT_STORAGE_DIR = File.join(TMP_DIR, 'repositories', __dir__)
DEFAULT_STORAGE_NAME = 'default'.freeze
TEST_REPO_PATH = File.join(DEFAULT_STORAGE_DIR, 'gitlab-test.git')
TEST_REPO_ORIGIN = '../_build/testrepos/gitlab-test.git'.freeze

module TestRepo
  def self.prepare_test_repository
    FileUtils.rm_rf(Dir["#{DEFAULT_STORAGE_DIR}/mutable-*"])

    FileUtils.mkdir_p(DEFAULT_STORAGE_DIR)

    {
      TEST_REPO_ORIGIN => TEST_REPO_PATH
    }.each do |origin, path|
      next if File.directory?(path)

      clone_new_repo!(origin, path)
    end
  end

  def test_repo_read_only
    Gitaly::Repository.new(storage_name: DEFAULT_STORAGE_NAME, relative_path: File.basename(TEST_REPO_PATH))
  end

  def rugged_from_gitaly(gitaly_repo)
    Rugged::Repository.new(repo_path_from_gitaly(gitaly_repo))
  end

  def repo_path_from_gitaly(gitaly_repo)
    storage_name = gitaly_repo.storage_name
    raise "this helper does not know storage #{storage_name.inspect}" unless storage_name == DEFAULT_STORAGE_NAME

    File.join(DEFAULT_STORAGE_DIR, gitaly_repo.relative_path)
  end

  def self.clone_new_repo!(origin, destination)
    env = {}
    env['GIT_EXEC_PATH'] = GIT_EXEC_PATH if defined?(GIT_EXEC_PATH)

    return if system(env, Gitlab.config.git.bin_path, "-c", "init.templateDir=", "clone", "--quiet", "--bare", origin.to_s, destination.to_s)

    abort "Failed to clone test repo. Try running 'make prepare-tests' and try again."
  end
end

TestRepo.prepare_test_repository
