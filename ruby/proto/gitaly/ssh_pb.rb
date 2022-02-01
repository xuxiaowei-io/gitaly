# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ssh.proto

require 'lint_pb'
require 'shared_pb'
require 'google/protobuf'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_file("ssh.proto", :syntax => :proto3) do
    add_message "gitaly.SSHUploadPackRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :stdin, :bytes, 2
      repeated :git_config_options, :string, 4
      optional :git_protocol, :string, 5
      optional :timeout_seconds, :int32, 6
    end
    add_message "gitaly.SSHUploadPackResponse" do
      optional :stdout, :bytes, 1
      optional :stderr, :bytes, 2
      optional :exit_status, :message, 3, "gitaly.ExitStatus"
    end
    add_message "gitaly.SSHUploadPackWithSidechannelRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      repeated :git_config_options, :string, 2
      optional :git_protocol, :string, 3
      optional :timeout_seconds, :int32, 4
    end
    add_message "gitaly.SSHUploadPackWithSidechannelResponse" do
    end
    add_message "gitaly.SSHReceivePackRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :stdin, :bytes, 2
      optional :gl_id, :string, 3
      optional :gl_repository, :string, 4
      optional :gl_username, :string, 5
      optional :git_protocol, :string, 6
      repeated :git_config_options, :string, 7
    end
    add_message "gitaly.SSHReceivePackResponse" do
      optional :stdout, :bytes, 1
      optional :stderr, :bytes, 2
      optional :exit_status, :message, 3, "gitaly.ExitStatus"
    end
    add_message "gitaly.SSHUploadArchiveRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :stdin, :bytes, 2
    end
    add_message "gitaly.SSHUploadArchiveResponse" do
      optional :stdout, :bytes, 1
      optional :stderr, :bytes, 2
      optional :exit_status, :message, 3, "gitaly.ExitStatus"
    end
  end
end

module Gitaly
  SSHUploadPackRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHUploadPackRequest").msgclass
  SSHUploadPackResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHUploadPackResponse").msgclass
  SSHUploadPackWithSidechannelRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHUploadPackWithSidechannelRequest").msgclass
  SSHUploadPackWithSidechannelResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHUploadPackWithSidechannelResponse").msgclass
  SSHReceivePackRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHReceivePackRequest").msgclass
  SSHReceivePackResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHReceivePackResponse").msgclass
  SSHUploadArchiveRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHUploadArchiveRequest").msgclass
  SSHUploadArchiveResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.SSHUploadArchiveResponse").msgclass
end
