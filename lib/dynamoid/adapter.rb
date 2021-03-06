# encoding: utf-8
module Dynamoid

  # Adapter provides a generic, write-through class that abstracts variations in the underlying connections to provide a uniform response
  # to Dynamoid.
  module Adapter
    extend self
    attr_accessor :tables

    # The actual adapter currently in use: presently AwsSdk.
    #
    # @since 0.2.0
    def adapter
      reconnect! unless @adapter
      @adapter
    end

    # Establishes a connection to the underyling adapter and caches all its tables for speedier future lookups. Issued when the adapter is first called.
    #
    # @since 0.2.0
    def reconnect!
      require "dynamoid/adapter/#{Dynamoid::Config.adapter}" unless Dynamoid::Adapter.const_defined?(Dynamoid::Config.adapter.camelcase)
      @adapter = Dynamoid::Adapter.const_get(Dynamoid::Config.adapter.camelcase)
      @adapter.connect! if @adapter.respond_to?(:connect!)
      self.tables = benchmark('Cache Tables', nil) {list_tables}
    end

    # Shows how long it takes a method to run on the adapter. Useful for generating logged output.
    #
    # @param [Symbol] method the name of the method to appear in the log
    # @param [Array] args the arguments to the method to appear in the log
    # @yield the actual code to benchmark
    #
    # @return the result of the yield
    #
    # @since 0.2.0
    def benchmark(method, table_name,  *args)
      start = Time.now
      stop = nil
      result = nil
      if table_name
        instrument_name = "dynamoid.#{table_name}.#{method.to_s.split('_').collect(&:downcase).join('.')}"
      else
        instrument_name = "dynamoid.#{method.to_s.split('_').collect(&:downcase).join('.')}"
      end
      ActiveSupport::Notifications.instrument (instrument_name) do
        result = yield
        stop = Time.now
      end
      Dynamoid.logger.info "(#{((stop - start) * 1000.0).round(2)} ms) #{method.to_s.split('_').collect(&:upcase).join(' ')}#{ " - #{args.inspect}" unless args.nil? || args.empty? }"
      return result
    end

    # Write an object to the adapter. Partition it to a randomly selected key first if necessary.
    #
    # @param [String] table the name of the table to write the object to
    # @param [Object] object the object itself
    # @param [Hash] options Options that are passed to the put_item call
    #
    # @return [Object] the persisted object
    #
    # @since 0.2.0
    def write(table, object, options = nil)
      if Dynamoid::Config.partitioning? && object[:id]
        object[:id] = "#{object[:id]}.#{Random.rand(Dynamoid::Config.partition_size)}"
        object[:updated_at] = Time.now.to_f
      end
      put_item(table,table, object, options)
    end

    # Read one or many keys from the selected table. This method intelligently calls batch_get or get on the underlying adapter depending on
    # whether ids is a range or a single key: additionally, if partitioning is enabled, it batch_gets all keys in the partition space
    # automatically. Finally, if a range key is present, it will also interpolate that into the ids so that the batch get will acquire the
    # correct record.
    #
    # @param [String] table the name of the table to write the object to
    # @param [Array] ids to fetch, can also be a string of just one id
    # @param [Hash] options: Passed to the underlying query. The :range_key option is required whenever the table has a range key,
    #                        unless multiple ids are passed in and Dynamoid::Config.partitioning? is turned off.
    #
    # @since 0.2.0
    def read(table, ids, options = {})
      range_key = options.delete(:range_key)

      if ids.respond_to?(:each)
        ids = ids.collect{|id| range_key ? [id, range_key] : id}
        if Dynamoid::Config.partitioning?
          results = batch_get_item(table, {table => id_with_partitions(ids)}, options)
          {table => result_for_partition(results[table],table)}
        else
          batch_get_item(table,{table => ids}, options)
        end
      else
        if Dynamoid::Config.partitioning?
          ids = range_key ? [[ids, range_key]] : ids
          results = batch_get_item(table,{table => id_with_partitions(ids)}, options)
          result_for_partition(results[table],table).first
        else
          options[:range_key] = range_key if range_key
          get_item(table, table, ids, options)
        end
      end
    end

    # Delete an item from a table. If partitioning is turned on, deletes all partitioned keys as well.
    #
    # @param [String] table the name of the table to write the object to
    # @param [Array] ids to delete, can also be a string of just one id
    # @param [Array] range_key of the record to delete, can also be a string of just one range_key
    #
    def delete(table, ids, options = {})
      range_key = options[:range_key] #array of range keys that matches the ids passed in
      if ids.respond_to?(:each)
        if range_key.respond_to?(:each)
          #turn ids into array of arrays each element being hash_key, range_key
          ids = ids.each_with_index.map{|id,i| [id,range_key[i]]}
        else
          ids = range_key ? [[ids, range_key]] : ids
        end
        
        if Dynamoid::Config.partitioning?
          batch_delete_item(table, table => id_with_partitions(ids))
        else
          batch_delete_item(table, table => ids)
        end
      else
        if Dynamoid::Config.partitioning?
          ids = range_key ? [[ids, range_key]] : ids
          batch_delete_item(table, table => id_with_partitions(ids))
        else
          delete_item(table, table, ids, options)
        end
      end
    end

    # Scans a table. Generally quite slow; try to avoid using scan if at all possible.
    #
    # @param [String] table the name of the table to write the object to
    # @param [Hash] scan_hash a hash of attributes: matching records will be returned by the scan
    #
    # @since 0.2.0
    def scan(table, query, opts = {})
      if Dynamoid::Config.partitioning?
        results = benchmark('Scan', table,  table, query) {adapter.scan(table, query, opts)}
        result_for_partition(results,table)
      else
        benchmark('Scan', table,  table, query) {adapter.scan(table, query, opts)}
      end
    end

    [:batch_get_item, :create_table, :delete_item, :delete_table, :get_item, :list_tables, :put_item].each do |m|
      # Method delegation with benchmark to the underlying adapter. Faster than relying on method_missing.
      #
      # @since 0.2.0
      define_method(m) do |table_name = "dynamoid", *args|
        benchmark("#{m.to_s}", table_name, args) {adapter.send(m, *args)}
      end
    end

    # Takes a list of ids and returns them with partitioning added. If an array of arrays is passed, we assume the second key is the range key
    # and pass it in unchanged.
    #
    # @example Partition id 1
    #   Dynamoid::Adapter.id_with_partitions(['1']) # ['1.0', '1.1', '1.2', ..., '1.199']
    # @example Partition id 1 and range_key 1.0
    #   Dynamoid::Adapter.id_with_partitions([['1', 1.0]]) # [['1.0', 1.0], ['1.1', 1.0], ['1.2', 1.0], ..., ['1.199', 1.0]]
    #
    # @param [Array] ids array of ids to partition
    #
    # @since 0.2.0
    def id_with_partitions(ids)
      Array(ids).collect {|id| (0...Dynamoid::Config.partition_size).collect{|n| id.is_a?(Array) ? ["#{id.first}.#{n}", id.last] : "#{id}.#{n}"}}.flatten(1)
    end
    
    #Get original id (hash_key) and partiton number from a hash_key
    #
    # @param [String] id the id or hash_key of a record, ex. xxxxx.13
    #
    # @return [String,String] original_id and the partition number, ex original_id = xxxxx partition = 13
    def get_original_id_and_partition id
      partition = id.split('.').last
      id = id.split(".#{partition}").first

      return id, partition
    end

    # Takes an array of query results that are partitioned, find the most recently updated ones that share an id and range_key, and return only the most recently updated. Compares each result by
    # their id and updated_at attributes; if the updated_at is the greatest, then it must be the correct result.
    #
    # @param [Array] returned partitioned results from a query
    # @param [String] table_name the name of the table
    #
    # @since 0.2.0
    def result_for_partition(results, table_name)
      table = adapter.get_table(table_name)
      
      if table.range_key     
        range_key_name = table.range_key.name.to_sym
        
        final_hash = {}

        results.each do |record|
          test_record = final_hash[record[range_key_name]]
          
          if test_record.nil? || ((record[range_key_name] == test_record[range_key_name]) && (record[:updated_at] > test_record[:updated_at]))
            #get ride of our partition and put it in the array with the range key
            record[:id], partition = get_original_id_and_partition  record[:id]
            final_hash[record[range_key_name]] = record
          end
        end
  
        return final_hash.values
      else
        {}.tap do |hash|
          Array(results).each do |result|
            next if result.nil?
            #Need to find the value of id with out the . and partition number
            id, partition = get_original_id_and_partition result[:id]
  
            if !hash[id] || (result[:updated_at] > hash[id][:updated_at])
              result[:id] = id
              hash[id] = result
            end
          end
        end.values
      end
    end

    # Delegate all methods that aren't defind here to the underlying adapter.
    #
    # @since 0.2.0
    def method_missing(method, *args, &block)
      return benchmark(method, "",  *args) {adapter.send(method, *args, &block)} if @adapter.respond_to?(method)
      super
    end
    # ADD the obj to the set of keys for a given index
    # We call this instead of reading existing ids and putting the item back. This ensures if multiple calls 
    # are updating the objects index the obj hash key isn't lost on an overwrite
    # partitioning is currently not supported we call the old method to deal with it
    def add_index_value(table_name, obj, opts = {})
      if Dynamoid::Config.partitioning?
       # existing = Dynamoid::Adapter.read(table_name, obj.hash_key, { :range_key => values[:range_value] })
        #ids = ((existing and existing[:ids]) or Set.new)
        #Dynamoid::Adapter.write(self.table_name, {:id => values[:hash_value], :ids => ids.merge([obj.hash_key]), :range => values[:range_value]})
      elsif Dynamoid::Config.remove_empty_index?
        key = opts.delete(:id)
        if obj.range_value
          add_block = Proc.new{|item| item.add(:ids => ["#{obj.hash_key}.#{obj.range_value}"])}
        else
          add_block = Proc.new{|item| item.add(:ids => ["#{obj.hash_key}"])}
        end
        
        adapter.update_item(table_name, key, opts, &add_block)
      else
        key = opts.delete(:id)
        if obj.range_value
          add_block = Proc.new{|item| item.add(:ids => ["#{obj.hash_key}.#{obj.range_value}"])}
        else
          add_block = Proc.new{|item| item.add(:ids => ["#{obj.hash_key}"])}
        end
        adapter.update_item(table_name, key, opts, &add_block)
      end
    end

    # DELETE the obj from the the set of keys for a given index
    def delete_index_value(table_name, obj, opts = {})
      if Dynamoid::Config.partitioning?
        #existing = Dynamoid::Adapter.read(self.table_name, values[:hash_value], { :range_key => values[:range_value]})
        #return true unless existing && existing[:ids] && existing[:ids].include?(obj.hash_key)
        #Dynamoid::Adapter.write(self.table_name, {:id => values[:hash_value], :ids => (existing[:ids] - Set[obj.hash_key]), :range => values[:range_value]})
      elsif Dynamoid::Config.remove_empty_index?
        key = opts.delete(:id)
        # do not lose our old options after we pass to update
        old_opts = opts.clone
        if obj.range_value
           delete_block = Proc.new{|item| item.delete(:ids => ["#{obj.hash_key}.#{obj.range_value}"])}
        else
           delete_block = Proc.new{|item| item.delete(:ids => ["#{obj.hash_key}"])}
        end
       
        result = adapter.update_item(table_name, key, opts, &delete_block)
        if !result.nil? && result["ids"].nil?
          begin
            # the index is not holding anything we must delete it
            # specifying unless exists makes sure we don't delete the index if another call added to the index
            adapter.delete_item(table_name,key, old_opts.merge({:unless_exists => :ids}))
            Dynamoid.logger.info("Removing empty index at #{key}")
          rescue AWS::DynamoDB::Errors::ConditionalCheckFailedException
            # we just return if the index is holding ids 
            return
          end
        end
      else
        key = opts.delete(:id)
        if obj.range_value
           delete_block = Proc.new{|item| item.delete(:ids => ["#{obj.hash_key}.#{obj.range_value}"])}
        else
           delete_block = Proc.new{|item| item.delete(:ids => ["#{obj.hash_key}"])}
        end
        adapter.update_item(table_name, key, opts, &delete_block)
      end
    end
    
    # Query the DynamoDB table. This employs DynamoDB's indexes so is generally faster than scanning, but is
    # only really useful for range queries, since it can only find by one hash key at once. Only provide
    # one range key to the hash. If paritioning is on, will run a query for every parition and join the results
    #
    # @param [String] table_name the name of the table
    # @param [Hash] opts the options to query the table with
    # @option opts [String] :hash_value the value of the hash key to find
    # @option opts [Range] :range_value find the range key within this range
    # @option opts [Number] :range_greater_than find range keys greater than this
    # @option opts [Number] :range_less_than find range keys less than this
    # @option opts [Number] :range_gte find range keys greater than or equal to this
    # @option opts [Number] :range_lte find range keys less than or equal to this
    #
    # @return [Array] an array of all matching items
    #
    # this needs to respond to a batch size
    def query(table_name, opts = {})
      
      unless Dynamoid::Config.partitioning?
        #no paritioning? just pass to the standard query method
        batch_size = opts.delete(:batch_size)
        if batch_size
        else
          adapter.query(table_name, opts)
        end
      else
        #get all the hash_values that could be possible
        ids = id_with_partitions(opts[:hash_value])

        #lets not overwrite with the original options
        modified_options = opts.clone     
        results = []
        
        #loop and query on each of the partition ids
        ids.each do |id|
          modified_options[:hash_value] = id

          query_result = adapter.query(table_name, modified_options)
          results += query_result.inject([]){|array, result| array += [result]} if query_result.any?
        end 

        result_for_partition results, table_name
      end
    end
  end
end
