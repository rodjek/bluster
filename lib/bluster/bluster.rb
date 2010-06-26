class Bluster
  attr_accessor :objects_path, :redis_host, :redis_port, :last_update_timestamp, :redis

  # Instantiate a new Bluster object.
  #
  # objects_path - path to the Nagios objects.cache file 
  #
  # Returns a configured Bluster instance.
  def initialize(objects_path, server)
    self.objects_path = objects_path
    self.redis_host = server.split(':').first
    self.redis_port = server.split(':').last.to_i
    self.last_update_timestamp = 0

    ensure_object_cache_exists
    ensure_redis_connection
  end
  
  # The list of contact object names.
  #
  # Returns an Array of String contact names.
  def contacts
    ensure_cache_up_to_date
    contacts = self.redis.keys("bluster:objects:contact:*")
    contacts.map! { |r| r.split(":")[3] }
    contacts.uniq
  end
  
  # The configuration details of a contact.
  #
  # contact - The String name of the contact to lookup.
  #
  # Returns a Hash of Strings.
  def get_contact(contact)
    ensure_cache_up_to_date
    namespace = "bluster:objects:contact:#{contact}"
    keys = self.redis.keys("#{namespace}:*")
    data = {}
    keys.each { |key|
      short_key = key.split(":")[4] 
      data[short_key] = self.redis.get("#{namespace}:#{short_key}")
    }
    data
  end
  
  # The list of command object names.
  #
  # Returns an Array of String command names.
  def commands
    ensure_cache_up_to_date
    commands = self.redis.keys("bluster:objects:command:*")
    commands.map! { |r| r.split(":")[3] }
    commands.uniq
  end
  
  # The configuration details of a command.
  #
  # command - The String name of the command to lookup.
  #
  # Returns a Hash of Strings.
  def get_command(command)
    ensure_cache_up_to_date
    namespace = "bluster:objects:command:#{command}"
    keys = self.redis.keys("#{namespace}:*")
    data = {}
    keys.each { |key|
      short_key = key.split(":")[4]
      data[short_key] = self.redis.get("#{namespace}:#{short_key}")
    }
    data
  end
  
  private
  
  # Checks that the provided object cache path exists.
  #
  # Returns nothing.
  def ensure_object_cache_exists
    if not File.exist? self.objects_path
      raise ObjectCacheNotFound.new(self.objects_path)
    end
  end
  
  # Ensure that the cache in Redis is up to date
  #
  # Returns nothing.
  def ensure_cache_up_to_date
    self.last_update_timestamp = self.redis.get("bluster:last_update_timestamp").to_i
    if self.last_update_timestamp.nil?
      update_object_cache
    elsif self.last_update_timestamp != File.new(self.objects_path).mtime.to_i
      update_object_cache
    end
  end
  
  # Ensure that a working connection to Redis has been established and that
  # the object cache is up to date.
  #
  # Returns nothing.
  def ensure_redis_connection
    self.redis = Redis.new(:host => self.redis_host, :port => self.redis_port)
    ensure_cache_up_to_date
  rescue Errno::ECONNREFUSED
    raise RedisConnectionError.new("Unable to connect to redis")
  end
  
  # Read the object cache file and store the contents in redis.
  #
  # Returns nothing.
  def update_object_cache
    in_object = false
    objects = {}
    data = {}
    type = ""
    File.open(self.objects_path, "r").readlines.each { |line|
      line = line.strip
      if line =~ %r{^define (\w+) .*}
        type = $1
        in_object = true
        data = {}
        if objects[type].nil?
          objects[type] = []
        end
      else
        if in_object == true
          if line == "}"
            in_object = false
            objects[type] << data
          else
            chunks = line.squeeze(' ').split(' ')
            data[chunks.first] = chunks[1..-1].join(' ')
          end
        end
      end
    }
    
    objects["contact"].each { |contact|
      namespace = "bluster:objects:contact:#{contact['contact_name']}"
      contact.keys.each { |key|
        self.redis.set("#{namespace}:#{key}", contact[key]) if key != "contact_name"
      }
    }
    
    objects["command"].each { |command|
      namespace = "bluster:objects:command:#{command['command_name']}"
      command.keys.each { |key|
        self.redis.set("#{namespace}:#{key}", command[key]) if key != "command_name"
      }
    }
    
    self.redis.set("bluster:last_update_timestamp", File.new(self.objects_path).mtime.to_i)
  end
end
