# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: conflicts.proto

require 'lint_pb'
require 'shared_pb'
require 'google/protobuf/timestamp_pb'
require 'google/protobuf'

Google::Protobuf::DescriptorPool.generated_pool.build do
  add_file("conflicts.proto", :syntax => :proto3) do
    add_message "gitaly.ListConflictFilesRequest" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :our_commit_oid, :string, 2
      optional :their_commit_oid, :string, 3
      optional :allow_tree_conflicts, :bool, 4
    end
    add_message "gitaly.ConflictFileHeader" do
      optional :commit_oid, :string, 2
      optional :their_path, :bytes, 3
      optional :our_path, :bytes, 4
      optional :our_mode, :int32, 5
      optional :ancestor_path, :bytes, 6
    end
    add_message "gitaly.ConflictFile" do
      oneof :conflict_file_payload do
        optional :header, :message, 1, "gitaly.ConflictFileHeader"
        optional :content, :bytes, 2
      end
    end
    add_message "gitaly.ListConflictFilesResponse" do
      repeated :files, :message, 1, "gitaly.ConflictFile"
    end
    add_message "gitaly.ResolveConflictsRequestHeader" do
      optional :repository, :message, 1, "gitaly.Repository"
      optional :our_commit_oid, :string, 2
      optional :target_repository, :message, 3, "gitaly.Repository"
      optional :their_commit_oid, :string, 4
      optional :source_branch, :bytes, 5
      optional :target_branch, :bytes, 6
      optional :commit_message, :bytes, 7
      optional :user, :message, 8, "gitaly.User"
      optional :timestamp, :message, 9, "google.protobuf.Timestamp"
    end
    add_message "gitaly.ResolveConflictsRequest" do
      oneof :resolve_conflicts_request_payload do
        optional :header, :message, 1, "gitaly.ResolveConflictsRequestHeader"
        optional :files_json, :bytes, 2
      end
    end
    add_message "gitaly.ResolveConflictsResponse" do
      optional :resolution_error, :string, 1
    end
  end
end

module Gitaly
  ListConflictFilesRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListConflictFilesRequest").msgclass
  ConflictFileHeader = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ConflictFileHeader").msgclass
  ConflictFile = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ConflictFile").msgclass
  ListConflictFilesResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ListConflictFilesResponse").msgclass
  ResolveConflictsRequestHeader = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ResolveConflictsRequestHeader").msgclass
  ResolveConflictsRequest = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ResolveConflictsRequest").msgclass
  ResolveConflictsResponse = ::Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.ResolveConflictsResponse").msgclass
end
