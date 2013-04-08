#!/usr/bin/env ruby
#
# Original one (file based) by R.I. Pienaar : http://www.devco.net
# Mongo version by Nicolas Szalay : http://www.rottenbytes.info
# Riak version by Gjalt van Rutten : http://www.bitbrains.nl

require 'rubygems'
require 'getoptlong'
require 'riak'

opts = GetoptLong.new(
                      [ '--interval', '-i', GetoptLong::REQUIRED_ARGUMENT],
                      [ '--host', '-h', GetoptLong::REQUIRED_ARGUMENT],
                      [ '--bucket', '-b', GetoptLong::REQUIRED_ARGUMENT],
                      [ '--protocol', '-p', GetoptLong::REQUIRED_ARGUMENT]
                      )

total = 0
old = 0
interval = 3600
riakhost="127.0.0.1"
riakprotocol="pbc"
bucket="mcollective"

opts.each do |opt, arg|
  case opt
  when '--interval'
    interval = arg.to_i
  when '--host'
    riakhost = arg
  when '--bucket'
    bucket = arg
  when '--protocol'
    riakprotocol = arg
  end
end

begin
  riak = Riak::Client.new(:nodes => [ {:host => riakhost, :protocol => riakprotocol}, ] )
  hosts = riak[bucket].get_index('$bucket', 'fqdn_bin').map { |k| riak[bucket][k].data }
rescue Exception => e
  fail "Error connecting to riak: #{e}"
end

hosts.each { |host|
  seen = host["lastseen"]
  fqdn = host["fqdn"]

  total += 1

  if (Time.now.to_i - seen)  > interval
    old+=1
  end
}

if old > 0
  puts("CRITICAL: #{old} / #{total} hosts not checked in within #{interval} seconds| totalhosts=#{total} oldhosts=#{old} currenthosts=#{total - old}")
  exit 2
else
  puts("OK: #{total} / #{total} hosts checked in within #{interval} seconds| totalhosts=#{total} oldhosts=#{old} currenthosts=#{total - old}")
  exit 0
end
