# frozen_string_literal: true

class ElasticSearchScanner
  include Enumerable

  SCROLL_PATH = '/_search/scroll'

  attr_reader :total_request_time,
              :total_elasticsearch_time

  def initialize(url, query, size=100, scroll_ttl = '1m', max_retries = 5)
    @url = url
    @query = query
    @size = size
    @scroll_ttl = scroll_ttl
    @max_retries = max_retries
    @fields_to_return = true # all fields
    @has_more = true
    @total_request_time = 0.0
    @total_elasticsearch_time = 0.0
  end

  def fields_to_return=(fields_to_return)
    @fields_to_return = fields_to_return
  end

  def each_batch
    yield search

    while has_more?
      yield scroll
    end

    nil
  end

  def each
    each_batch do |results|
      results.each do |result|
        yield result
      end
    end
  end

  def has_more?
    @has_more
  end

  private

  def search_payload
    {
      size: @size,
      _source: @fields_to_return,
      query: @query
    }
  end

  def search
    uri = URI(@url + '?scroll=' + @scroll_ttl)
    http = Net::HTTP.new(uri.host, uri.port)
    req = Net::HTTP::Post.new(uri.request_uri, 'Content-Type' => 'application/json')
    req.body = search_payload.to_json

    make_request(http, req)
  end

  def scroll
    uri = URI(@url)
    http = Net::HTTP.new(uri.host, uri.port)
    req = Net::HTTP::Post.new(SCROLL_PATH, 'Content-Type' => 'application/json')
    req.body = {
      scroll: @scroll_ttl,
      scroll_id: @scroll_id
    }.to_json

    make_request(http, req)
  end

  def make_request(http, req)
    request_started_at = Time.now
    res = http.request(req)
    @total_request_time += (Time.now - request_started_at)
    data = JSON.parse(res.body)
    @total_elasticsearch_time += data['took']
    @scroll_id = data['_scroll_id']
    @has_more = data['hits']['hits'].size == @size
    data['hits']['hits']
  rescue Net::ReadTimeout => e
    attempts ||= 0
    attempts += 1
    if attempts < @max_retries
      sleep([0.1, 0.2, 0.4, 1.0][attempts-1] || 1.0)
      retry
    end
    raise e
  end
end
