require 'licensee'

module GitalyServer
  class RepositoryService < Gitaly::RepositoryService::Service
    include Utils

    def find_license(_request, call)
      path = GitalyServer.repo_path(call)

      begin
        project = ::Licensee.project(path)
        return Gitaly::FindLicenseResponse.new(license_short_name: "") unless project&.license

        license = project.license

        return Gitaly::FindLicenseResponse.new(
          license_short_name: license.key || "",
          license_name: license.name || "",
          license_url: license.url || "",
          license_path: project.matched_file&.filename,
          license_nickname: license.nickname || ""
        ).tap do |resp|
          if license.key == "other"
            resp.license_nickname = "LICENSE"
            resp.license_url = ""
          end
        end
      rescue Rugged::Error
      end

      Gitaly::FindLicenseResponse.new(license_short_name: "")
    end
  end
end
