# encoding: utf-8
module Dynamoid #:nodoc:
  module Indexes

    # The class contains all the information an index contains, including its keys and which attributes it covers.
    class Index
      attr_accessor :source, :name, :hash_keys, :range_keys
      alias_method :range_key?, :range_keys
      
      # Create a new index. Pass either :range => true or :range => :column_name to create a ranged index on that column.
      #
      # @param [Class] source the source class for the index
      # @param [Symbol] name the name of the index
      #
      # @since 0.2.0      
      def initialize(source, name, options = {})
        @source = source
        
        if options.delete(:range)
          @range_keys = sort(name)
        elsif options[:range_key]
          @range_keys = sort(options[:range_key])
        end

        @hash_keys = sort(name)
        @name = sort([hash_keys, range_keys])
        
        raise Dynamoid::Errors::InvalidField, 'A key specified for an index is not a field' unless keys.all?{|n| source.attributes.include?(n)}
      end
      
      # Sort objects into alphabetical strings, used for composing index names correctly (since we always assume they're alphabetical).
      #
      # @example find all users by first and last name
      #   sort([:gamma, :alpha, :beta, :omega]) # => [:alpha, :beta, :gamma, :omega]
      #
      # @since 0.2.0         
      def sort(objs)
        Array(objs).flatten.compact.uniq.collect(&:to_s).sort.collect(&:to_sym)
      end

      # Return the array of keys this index uses for its table.
      #
      # @since 0.2.0      
      def keys
        [Array(hash_keys) + Array(range_keys)].flatten.uniq
      end
      
      # Return the table name for this index.
      #
      # @since 0.2.0
      def table_name
        "#{Dynamoid::Config.namespace}_index_" + source.table_name.sub("#{Dynamoid::Config.namespace}_", '').singularize + "_#{name.collect(&:to_s).collect(&:pluralize).join('_and_')}"
      end

      # Given either an object or a list of attributes, generate a hash key and a range key for the index. Optionally pass in 
      # true to changed_attributes for a list of all the object's dirty attributes in convenient index form (for deleting stale 
      # information from the indexes).
      # if changed_attributes is true we only check to see if the attributes that have changed correspond to the current index
      #
      # @param [Object] attrs either an object that responds to :attributes, or a hash of attributes
      #
      # @return [Hash] a hash with the keys :hash_value and :range_value
      #
      # @since 0.2.0
      def values(attrs, changed_attributes = false)
        changed_hash = {}
        if changed_attributes
          changed_attrs = attrs.changes.delete_if {|k,v| v.first == v.last}
          changed = changed_attrs.map{|k,v| k}.to_set
          if !self.range_keys.nil?
            return changed_hash if !(!(self.hash_keys.map(&:to_s).to_set & changed).empty? || !(self.range_keys.map(&:to_s).to_set & changed).empty?)
          else 
            return changed_hash if !(!(self.hash_keys.map(&:to_s).to_set & changed).empty?)
          end
          changed_attrs.each {|k, v| changed_hash[k.to_sym] = (v.first || v.last)}
        end
        attrs = attrs.send(:attributes) if attrs.respond_to?(:attributes)
        source_attributes = self.source.attributes
        {}.tap do |hash|
          hash[:hash_value] = self.hash_keys.collect{|key| (if changed_hash[key]; changed_hash[key]; else attrs[key]; end)}.join('.')
          # check to see if we have a string in the range_keys
          # could make it simpler if the limit for a range_key is based on one attribute
          if self.range_key?
            if self.range_keys.select{|v| !source_attributes[v].nil? && source_attributes[v][:type] == :string}.any?
              hash[:range_value] = self.range_keys.inject("") {|sum, key| sum + if changed_hash[key]; changed_hash[key].to_s; else attrs[key].to_s; end} 
            else
              hash[:range_value] = self.range_keys.inject(0.0) {|sum, key| sum + if changed_hash[key]; changed_hash[key].to_f; else attrs[key].to_f; end}
            end
          end
        end
      end
      
      # Save an object to this index, merging it with existing ids if there's already something present at this index location.
      # First, though, delete this object from its old indexes (so the object isn't listed in an erroneous index).
      # We only want to save the object if the values for the index has changed
      # @since 0.2.0
      def save(obj)
        return true if !obj.new_record? &&  values(obj,true).empty?
        self.delete(obj, true) if !obj.new_record? && obj.changed?
        values = values(obj)
        return true if values[:hash_value].blank? || (!values[:range_value].nil? && values[:range_value].blank?)
        Dynamoid::Adapter.add_index_value(self.table_name,obj,{:id => values[:hash_value], :range_key => values[:range_value]})  
      end

      # Delete an object from this index, preserving existing ids if there are any, and failing gracefully if for some reason the 
      # index doesn't already have this object in it.
      #
      # @since 0.2.0      
      def delete(obj, changed_attributes = false)
        values = values(obj, changed_attributes)
        return true if values[:hash_value].blank? || (!values[:range_value].nil? && values[:range_value].blank?)
        Dynamoid::Adapter.delete_index_value(self.table_name,obj,{:id => values[:hash_value], :range_key => values[:range_value]})
      end
      
    end
  end
end
