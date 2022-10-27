module GitalyServer
  module Utils
    # See internal/logsanitizer/url.go for credits and explanation.
    URL_HOST_PATTERN = %r{([a-z][a-z0-9+\-.]*://)?([a-z0-9\-._~%!$&'()*+,;=:]+@)([a-z0-9\-._~%]+|\[[a-z0-9\-._~%!$&'()*+,;=:]+\])}i.freeze

    def sanitize_url(str)
      str.gsub(URL_HOST_PATTERN, '\1[FILTERED]@\3\4')
    end
  end
end
