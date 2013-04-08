metadata    :name        => "riakdb",
            :description => "RIAK based discovery for databases built using registration",
            :author      => "Gjalt van Rutten",
            :license     => "GPL",
            :version     => "0.2",
            :url         => "http://www.bitbrains.nl/",
            :timeout     => 0

discovery do
	capabilities [:facts, :identity, :agent]
end

