# frozen_string_literal: true

# Sidekiq's bin/webload times one page, /busy. This times the page PAGE names the same way, so
# that each page benchmarked for Hench has a Sidekiq counterpart: Sidekiq::Web in process
# through Rack::Test, its middlewares cleared as bin/webload clears them, 100 warm-up requests,
# then COUNT (10,000) timed requests, each of which must answer 200. Redis is REDIS_URL.
#
# Sidekiq 8.1's web UI needs a Rack session (it reads the visitor's locale from it), which
# bin/webload does not set up: its first request fails. Each request here carries an empty
# session hash, the cheapest session there is (a cookie session would decode and sign one).
require "bundler/setup"
require "sidekiq/web"
require "rack/test"

Sidekiq::Web.configure do |config|
  config.middlewares.clear
end

class WebLoad
  include Rack::Test::Methods

  def app
    @app ||= Sidekiq::Web.new
  end

  def hit(page)
    get page, {}, {"rack.session" => {}}
    raise "#{page}: #{last_response.status}" unless last_response.status == 200
  end
end

page = ENV.fetch("PAGE", "/busy")
count = Integer(ENV.fetch("COUNT", "10000"))
web = WebLoad.new
100.times { web.hit(page) }
started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
count.times { web.hit(page) }
puts "#{page} in #{Process.clock_gettime(Process::CLOCK_MONOTONIC) - started} sec"
