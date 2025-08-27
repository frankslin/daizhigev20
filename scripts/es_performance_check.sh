#!/bin/bash

echo "=== Elasticsearch Performance Diagnostics ==="
echo "Date: $(date)"
echo

echo "1. System Resources:"
echo "Memory:"
free -h
echo -e "\nCPU Load:"
uptime
echo -e "\nDisk Space:"
df -h | grep -E "(Filesystem|/dev/|elasticsearch)"
echo

echo "2. Elasticsearch Cluster Health:"
curl -s "localhost:9200/_cluster/health" | jq '.'
echo

echo "3. Node Stats Summary:"
curl -s "localhost:9200/_nodes/stats" | jq '{
  cluster_name: .cluster_name,
  nodes: (.nodes | to_entries[] | {
    name: .value.name,
    heap_used_percent: .value.jvm.mem.heap_used_percent,
    heap_max: .value.jvm.mem.heap_max_in_bytes,
    cpu_percent: .value.os.cpu.percent,
    load_average: .value.os.cpu.load_average
  })
}'
echo

echo "4. Index Stats:"
curl -s "localhost:9200/_cat/indices/chinese-classics?format=json" | jq '.[] | {
  index: .index,
  docs_count: .["docs.count"],
  store_size: .["store.size"], 
  segments_count: .["segments.count"],
  segments_memory: .["segments.memory"]
}'
echo

echo "5. JVM Memory Details:"
curl -s "localhost:9200/_nodes/stats/jvm" | jq '.nodes | to_entries[] | {
  node: .value.name,
  heap_used_percent: .value.jvm.mem.heap_used_percent,
  heap_used_mb: (.value.jvm.mem.heap_used_in_bytes / 1024 / 1024 | round),
  heap_max_mb: (.value.jvm.mem.heap_max_in_bytes / 1024 / 1024 | round),
  gc_collection_count: .value.jvm.gc.collectors.young.collection_count,
  gc_collection_time_ms: .value.jvm.gc.collectors.young.collection_time_in_millis
}'
echo

echo "6. Query Performance Indicators:"
curl -s "localhost:9200/_nodes/stats/indices" | jq '.nodes | to_entries[] | {
  node: .value.name,
  search_query_total: .value.indices.search.query_total,
  search_query_time_ms: .value.indices.search.query_time_in_millis,
  avg_query_time_ms: ((.value.indices.search.query_time_in_millis / .value.indices.search.query_total) | round),
  query_cache_hit_count: .value.indices.query_cache.hit_count,
  query_cache_miss_count: .value.indices.query_cache.miss_count
}'
echo

echo "7. Segment Information:"
curl -s "localhost:9200/_cat/segments/chinese-classics?format=json" | jq 'sort_by(-.size_in_bytes) | .[:10] | .[] | {
  segment: .segment,
  size: .size,
  docs_count: .docs_count,
  committed: .committed
}'

echo
echo "=== End of Diagnostics ==="
