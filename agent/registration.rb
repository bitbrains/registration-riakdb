module MCollective
  module Agent
    class Registration
      attr_reader :timeout, :meta

      def initialize
        @meta = {:license => "GPL",
          :author => "Gjalt van Rutten <grutten@bitbrains.nl>",
          :url => "http://www.bitbrains.nl",
          :version => 0.1 }

        require 'riak'

        @timeout = 2

        @config = Config.instance

        @riakhost = @config.pluginconf["registration.riakhost"] || [ {:host => "127.0.0.1", :protocol => "pbc"}, ]
        @riakbucket = @config.pluginconf["registration.riakbucket"] || "mcollective"
        @yaml_dir = @config.pluginconf["registration.extra_yaml_dir"] || false

        Log.instance.info("We have #{@riakhost.length} riak hosts configured")
        begin
          @riakhost.map { |host| Log.instance.info("Connecting to riak @ #{host[:host]} bucket #{@riakbucket}") }
          @client = Riak::Client.new(:nodes => @riakhost)
        rescue Exception => e
          Log.instance.error("Failed to connect to riak: #{e}")
        end
      end

      def handlemsg(msg, connection)
        if ! @client.ping 
          Log.instance.warn("Riak not alive, bailing out")
          return nil
        end

        req = msg[:body]

        if (req.kind_of?(Array))
          Log.instance.warn("Got no facts - did you forget to add 'registration = Meta' to your server.cfg?");
          return nil
        end

        req[:fqdn] = req[:facts]["fqdn"]
        req[:lastseen] = Time.now.to_i

        if (@yaml_dir != false)
          req[:extra] = {}
          Dir[@yaml_dir + "/*.yaml"].each do | f |
            req[:extra][File.basename(f).split('.')[0]] = YAML.load_file(f)
          end
        end

        if req[:fqdn].nil?
          Log.instance.debug("Got stats without a FQDN in facts")
          return nil
        end

        before = Time.now.to_f
        begin
          kv_node = @client[@riakbucket].get_or_new(req[:fqdn])
          kv_node.data = req
          kv_node.indexes['fqdn_bin'] << req[:fqdn]
          kv_node.store
        rescue Exception => e
	        fail "An error has occured storing data in riak: #{e}"
        ensure
          after = Time.now.to_f
          Log.instance.debug("Updated data for host #{req[:fqdn]} in #{after - before} seconds")
        end

        nil
      end

      def help
      end
    end
  end
end