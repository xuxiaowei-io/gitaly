$:.unshift(File.expand_path('../proto', __dir__))
require 'gitaly'

require 'securerandom'
require 'gitlab-labkit'
require 'rugged'

require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/numeric/bytes'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/integer/time'
require 'active_support/core_ext/module/delegation'
require 'active_support/core_ext/enumerable'

require_relative './gitlab/config.rb'

require_relative 'gitaly_server/utils.rb'
require_relative 'gitaly_server/repository_service.rb'
require_relative 'gitaly_server/health_service.rb'

module GitalyServer
  REPO_PATH_HEADER = 'gitaly-repo-path'.freeze
  GITALY_SERVERS_HEADER = 'gitaly-servers'.freeze

  def self.repo_path(call)
    call.metadata.fetch(REPO_PATH_HEADER)
  end

  def self.register_handlers(server)
    server.handle(RepositoryService.new)
    server.handle(HealthService.new)
  end
end
