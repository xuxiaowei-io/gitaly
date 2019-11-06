# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: wiki.proto

require 'google/protobuf'

require 'shared_pb'
Google::Protobuf::DescriptorPool.generated_pool.build do
  add_message "gitaly.WikiCommitDetails" do
    optional :name, :bytes, 1
    optional :email, :bytes, 2
    optional :message, :bytes, 3
    optional :user_id, :int32, 4
    optional :user_name, :bytes, 5
  end
  add_message "gitaly.WikiPageVersion" do
    optional :commit, :message, 1, "gitaly.GitCommit"
    optional :format, :string, 2
  end
  add_message "gitaly.WikiPage" do
    optional :version, :message, 1, "gitaly.WikiPageVersion"
    optional :format, :string, 2
    optional :title, :bytes, 3
    optional :url_path, :string, 4
    optional :path, :bytes, 5
    optional :name, :bytes, 6
    optional :historical, :bool, 7
    optional :raw_data, :bytes, 8
  end
  add_message "gitaly.WikiGetPageVersionsRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :page_path, :bytes, 2
    optional :page, :int32, 3
    optional :per_page, :int32, 4
  end
  add_message "gitaly.WikiGetPageVersionsResponse" do
    repeated :versions, :message, 1, "gitaly.WikiPageVersion"
  end
  add_message "gitaly.WikiWritePageRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :name, :bytes, 2
    optional :format, :string, 3
    optional :commit_details, :message, 4, "gitaly.WikiCommitDetails"
    optional :content, :bytes, 5
  end
  add_message "gitaly.WikiWritePageResponse" do
    optional :duplicate_error, :bytes, 1
  end
  add_message "gitaly.WikiUpdatePageRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :page_path, :bytes, 2
    optional :title, :bytes, 3
    optional :format, :string, 4
    optional :commit_details, :message, 5, "gitaly.WikiCommitDetails"
    optional :content, :bytes, 6
  end
  add_message "gitaly.WikiUpdatePageResponse" do
    optional :error, :bytes, 1
  end
  add_message "gitaly.WikiDeletePageRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :page_path, :bytes, 2
    optional :commit_details, :message, 3, "gitaly.WikiCommitDetails"
  end
  add_message "gitaly.WikiDeletePageResponse" do
  end
  add_message "gitaly.WikiFindPageRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :title, :bytes, 2
    optional :revision, :bytes, 3
    optional :directory, :bytes, 4
  end
  add_message "gitaly.WikiFindPageResponse" do
    optional :page, :message, 1, "gitaly.WikiPage"
  end
  add_message "gitaly.WikiFindFileRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :name, :bytes, 2
    optional :revision, :bytes, 3
  end
  add_message "gitaly.WikiFindFileResponse" do
    optional :name, :bytes, 1
    optional :mime_type, :string, 2
    optional :raw_data, :bytes, 3
    optional :path, :bytes, 4
  end
  add_message "gitaly.WikiGetAllPagesRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :limit, :uint32, 2
    optional :direction_desc, :bool, 3
    optional :sort, :enum, 4, "gitaly.WikiGetAllPagesRequest.SortBy"
  end
  add_enum "gitaly.WikiGetAllPagesRequest.SortBy" do
    value :TITLE, 0
    value :CREATED_AT, 1
  end
  add_message "gitaly.WikiGetAllPagesResponse" do
    optional :page, :message, 1, "gitaly.WikiPage"
    optional :end_of_page, :bool, 2
  end
  add_message "gitaly.WikiListPagesRequest" do
    optional :repository, :message, 1, "gitaly.Repository"
    optional :limit, :uint32, 2
    optional :direction_desc, :bool, 3
    optional :sort, :enum, 4, "gitaly.WikiListPagesRequest.SortBy"
    optional :offset, :uint32, 5
  end
  add_enum "gitaly.WikiListPagesRequest.SortBy" do
    value :TITLE, 0
    value :CREATED_AT, 1
  end
  add_message "gitaly.WikiListPagesResponse" do
    optional :page, :message, 1, "gitaly.WikiPage"
  end
end

module Gitaly
  WikiCommitDetails = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiCommitDetails").msgclass
  WikiPageVersion = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiPageVersion").msgclass
  WikiPage = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiPage").msgclass
  WikiGetPageVersionsRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiGetPageVersionsRequest").msgclass
  WikiGetPageVersionsResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiGetPageVersionsResponse").msgclass
  WikiWritePageRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiWritePageRequest").msgclass
  WikiWritePageResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiWritePageResponse").msgclass
  WikiUpdatePageRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiUpdatePageRequest").msgclass
  WikiUpdatePageResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiUpdatePageResponse").msgclass
  WikiDeletePageRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiDeletePageRequest").msgclass
  WikiDeletePageResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiDeletePageResponse").msgclass
  WikiFindPageRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiFindPageRequest").msgclass
  WikiFindPageResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiFindPageResponse").msgclass
  WikiFindFileRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiFindFileRequest").msgclass
  WikiFindFileResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiFindFileResponse").msgclass
  WikiGetAllPagesRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiGetAllPagesRequest").msgclass
  WikiGetAllPagesRequest::SortBy = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiGetAllPagesRequest.SortBy").enummodule
  WikiGetAllPagesResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiGetAllPagesResponse").msgclass
  WikiListPagesRequest = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiListPagesRequest").msgclass
  WikiListPagesRequest::SortBy = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiListPagesRequest.SortBy").enummodule
  WikiListPagesResponse = Google::Protobuf::DescriptorPool.generated_pool.lookup("gitaly.WikiListPagesResponse").msgclass
end
