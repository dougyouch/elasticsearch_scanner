# frozen_string_literal: true

class ElasticSearchFacetScanner
  include Enumerable

  SCROLL_PATH = '/_search/scroll'

  attr_reader :total_request_time,
              :total_elasticsearch_time

  def initialize(url, field, size=100, max_retries = 5)
    @url = url
    @field = field
    @aggregation_name = "#{field}_terms"
    @size = size
    @max_retries = max_retries
    @has_more = true
    @total_request_time = 0.0
    @total_elasticsearch_time = 0.0
  end

  def each_batch
    yield search

    while has_more?
      yield search
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
      size: 0,
      aggs: {
        @aggregation_name => {
          terms: {
            field: @field,
            size: @size,
            order: {
              _term: :asc
            }
          }
        }
      }
    }
  end

  def search_payload_with_range
    search_payload.merge(
      query: {
        range: {
          @field => {
           gt:  @field_max_value
          }
        }
      }
    )
  end

  def search
    uri = URI(@url)
    http = Net::HTTP.new(uri.host, uri.port)
    req = Net::HTTP::Post.new(uri.request_uri, 'Content-Type' => 'application/json')
    req.body = (@field_max_value ? search_payload_with_range : search_payload).to_json

    make_request(http, req)
  end

  def make_request(http, req)
    request_started_at = Time.now
    res = http.request(req)
    @total_request_time += (Time.now - request_started_at)
    data = JSON.parse(res.body)
    @total_elasticsearch_time += data['took']
    aggregation = data['aggregations'][@aggregation_name]
    @has_more = aggregation['sum_other_doc_count'] > 0
    @field_max_value = aggregation['buckets'].size > 0 ? aggregation['buckets'].last['key'] : nil
    aggregation['buckets']
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
