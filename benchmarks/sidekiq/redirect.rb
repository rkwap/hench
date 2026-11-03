# frozen_string_literal: true

# Loaded with `ruby -r` before Sidekiq's own bin/sidekiqload (benchmarks/run.sh does it). The
# script talks to Redis on two hard-coded ports: 6379, and 6380 for the toxiproxy proxy it puts
# in front of 6379. This sends both to the benchmark's private servers instead,
# BENCH_REDIS_PORT and BENCH_PROXY_PORT, so that the script runs unchanged without touching a
# Redis that may already listen on 6379. Nothing else is changed.
require "bundler/setup"
require "sidekiq"
require "toxiproxy"

module BenchRedirect
  PORTS = {
    6379 => Integer(ENV.fetch("BENCH_REDIS_PORT")),
    6380 => Integer(ENV.fetch("BENCH_PROXY_PORT"))
  }.freeze

  def self.port(port)
    PORTS.fetch(Integer(port), port)
  end

  def self.address(address)
    host, port = address.split(":")
    "#{host}:#{port(port)}"
  end

  # The script's `config.redis = {db: 13, port: ...}`.
  module Config
    def redis=(hash)
      hash = hash.merge(port: BenchRedirect.port(hash[:port])) if hash.key?(:port)
      super(hash)
    end
  end

  # The script's `Toxiproxy.populate([{name:, listen:, upstream:}])`.
  module Populate
    def populate(*proxies)
      proxies = proxies.first if proxies.first.is_a?(Array)
      super(proxies.map do |proxy|
        proxy.merge(
          listen: BenchRedirect.address(proxy[:listen]),
          upstream: BenchRedirect.address(proxy[:upstream])
        )
      end)
    end
  end
end

Sidekiq::Config.prepend(BenchRedirect::Config)
Toxiproxy.singleton_class.prepend(BenchRedirect::Populate)
