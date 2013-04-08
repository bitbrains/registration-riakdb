module MCollective
  class Discovery
    class Riakdb
      require 'riak'

      class << self
        def discover(filter, timeout, limit=0, client=nil)
          config = Config.instance

          riakhost = config.pluginconf["registration.riakhost"] || [ {:host => "127.0.0.1", :protocol => "pbc"}, ]
          bucket = config.pluginconf["registration.riakbucket"] || "mcollective"
          newerthan = Time.now.to_i - Integer(config.pluginconf["registration.criticalage"] || 3600)

          begin
            riak = Riak::Client.new(:nodes => riakhost)
            hosts = riak[bucket].get_index('$bucket', 'fqdn_bin').map { |k| riak[bucket][k].data }
          rescue Exception => e
            fail "Error connecting to riak: #{e}"
          end

          found = []

          filter.keys.each do |key|
            case key 
              when "identity"
                found << identity_search(filter["identity"], hosts, newerthan)
              when "fact"
                found << fact_search(filter["fact"], hosts, newerthan)
              when "agent"
                found << agent_search(filter["agent"], hosts, newerthan)
            end unless filter[key].empty?
          end

          return found.inject(found[0]){|x, y| x & y}
        end

        def identity_search(filter, hosts, newerthan)
          nodes = []
          found = []

          hosts.map { |host| nodes << host["identity"] if (host["lastseen"].to_i > newerthan) }

          unless filter.empty?
            filter.each do |identity|
              identity = regexy_string(identity)

              if identity.is_a?(Regexp)
                found = nodes.grep(identity)
              elsif nodes.include?(identity)
                found << identity
              end 
            end 
          end 
          
          return found
        end
        
        def agent_search(filter, hosts, newerthan)
          found = []
          
          unless filter.empty?
            hosts.map do |host|
              filter.uniq.each do |agent|
                found << host["identity"] if host["agentlist"].include?(agent) && host["lastseen"].to_i > newerthan
              end
            end
          end
          
          return found
        end
        
        def fact_search(filters, hosts, newerthan)
          found = []

          unless filters.empty?
            hosts.map do |host|
              filters.each do |filter|
                if host["facts"].include?(filter[:fact])
                  case filter[:operator]
                  when "==", "=~"
                    if regexy_string(filter[:value]).is_a?(Regexp)
                      found << host["identity"] if host["facts"][ filter[:fact] ] =~ regexy_string(filter[:value])
                    else
                      found << host["identity"] if host["facts"][ filter[:fact] ] == filter[:value]
                    end
                  when "<="
                    found << host["identity"] if host["facts"][ filter[:fact] ] <= filter[:value]
                  when ">="
                    found << host["identity"] if host["facts"][ filter[:fact] ] >= filter[:value]
                  when ">"
                    found << host["identity"] if host["facts"][ filter[:fact] ] > filter[:value]
                  when "<"
                    found << host["identity"] if host["facts"][ filter[:fact] ] < filter[:value]
                  when "!="
                    found << host["identity"] if host["facts"][ filter[:fact] ] != filter[:value]
                  else
                    raise "Cannot perform %s matches for facts using the riakdb discovery method" % filter[:operator]
                  end
                end
              end if host["lastseen"].to_i > newerthan
            end 
          end
          
          return found
        end

        def regexy_string(string)
          if string.match("^/")
            Regexp.new(string.gsub("\/", ""))
          else
            string
          end
        end
      end
    end
  end
end