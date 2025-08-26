# 🔍 Elasticsearch 中国古典文献搜索引擎部署指南

## 📋 项目概述

本项目基于 Elasticsearch 构建了一个中国古典文献全文搜索引擎，包含佛藏、儒藏、医藏、史藏等10个类别共计15,694个传统典籍文件，总大小约4.8GB。支持高性能全文检索和分类浏览。

- [示例页面](https://cn-classics.xishiduliu.com/)

### 🎯 项目特性

- **海量文献**: 15,694个古典文献文本文件
- **十大分类**: 佛藏、儒藏、医藏、史藏、子藏、易藏、艺藏、诗藏、道藏、集藏
- **全文搜索**: 毫秒级响应的关键词搜索
- **智能高亮**: 搜索结果关键词自动高亮
- **响应式界面**: 支持PC、平板、手机访问
- **安全配置**: 仅本地访问，通过SSH隧道对外服务

## 🖥️ 系统要求

### 硬件配置
- **内存**: 最低6GB，推荐8GB以上
- **存储**: 50GB以上可用空间（SSD推荐）
- **CPU**: 2核以上
- **网络**: 稳定的互联网连接

### 软件环境
- **操作系统**: Ubuntu 22.04 LTS
- **Java**: OpenJDK 17
- **Python**: Python 3.10+
- **Elasticsearch**: 8.19.2

## 🚀 部署步骤

### 1. 系统准备

```bash
# 更新系统
sudo apt update && sudo apt upgrade -y

# 安装Java 17
sudo apt install openjdk-17-jre-headless -y

# 验证Java安装
java -version
```

### 2. 安装Elasticsearch

```bash
# 安装必要工具
sudo apt install wget gnupg apt-transport-https -y

# 添加Elasticsearch GPG密钥
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg

# 添加Elasticsearch仓库
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

# 更新包列表并安装
sudo apt update
sudo apt install elasticsearch -y
```

### 3. 系统优化配置

```bash
# 设置虚拟内存映射数量（Elasticsearch必需）
echo 'vm.max_map_count=262144' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p

# 设置文件描述符限制
echo 'elasticsearch soft nofile 65536' | sudo tee -a /etc/security/limits.conf
echo 'elasticsearch hard nofile 65536' | sudo tee -a /etc/security/limits.conf

# 配置内存锁定
sudo mkdir -p /etc/systemd/system/elasticsearch.service.d
sudo tee /etc/systemd/system/elasticsearch.service.d/override.conf << 'EOF'
[Service]
LimitMEMLOCK=infinity
EOF
```

### 4. Elasticsearch配置

```bash
# 备份原配置
sudo cp /etc/elasticsearch/elasticsearch.yml /etc/elasticsearch/elasticsearch.yml.backup

# 创建优化配置
sudo tee /etc/elasticsearch/elasticsearch.yml << 'EOF'
# 集群和节点名称
cluster.name: text-search-cluster
node.name: node-1

# 路径配置
path.data: /var/lib/elasticsearch
path.logs: /var/log/elasticsearch

# 内存锁定
bootstrap.memory_lock: true

# 网络配置（安全：仅本地访问）
network.host: 127.0.0.1
http.port: 9200

# 单节点模式
discovery.type: single-node

# 安全配置（开发模式）
xpack.security.enabled: false
xpack.security.enrollment.enabled: false
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false

# 禁用ML功能节省资源
xpack.ml.enabled: false

# 索引优化配置
indices.memory.index_buffer_size: 30%
indices.memory.min_index_buffer_size: 96mb
indices.queries.cache.size: 25%
indices.fielddata.cache.size: 25%

# 其他优化
action.destructive_requires_name: true
cluster.routing.allocation.disk.threshold_enabled: true
cluster.routing.allocation.disk.watermark.low: 85%
cluster.routing.allocation.disk.watermark.high: 90%

# 允许跨域访问（用于Web前端）
http.cors.enabled: true
http.cors.allow-origin: "*"
http.cors.allow-headers: "X-Requested-With,Content-Type,Content-Length,Authorization"
EOF
```

### 5. JVM内存配置

```bash
# 创建内存配置（根据服务器内存调整）
sudo mkdir -p /etc/elasticsearch/jvm.options.d

# 对于8GB+内存的服务器
sudo tee /etc/elasticsearch/jvm.options.d/heap.options << 'EOF'
-Xms6g
-Xmx6g
EOF

# 对于4-6GB内存的服务器，使用：
# echo -e "-Xms2g\n-Xmx2g" | sudo tee /etc/elasticsearch/jvm.options.d/heap.options
```

### 6. 启动Elasticsearch服务

```bash
# 重载systemd配置
sudo systemctl daemon-reload

# 启用开机自启
sudo systemctl enable elasticsearch

# 启动服务
sudo systemctl start elasticsearch

# 检查服务状态
sudo systemctl status elasticsearch

# 等待服务完全启动（15秒）
sleep 15

# 验证安装
curl -X GET "localhost:9200/"
```

成功响应示例：
```json
{
  "name" : "node-1",
  "cluster_name" : "text-search-cluster",
  "cluster_uuid" : "CavAp3LdSU-dV2-W9BoN8Q",
  "version" : {
    "number" : "8.19.2"
  },
  "tagline" : "You Know, for Search"
}
```

## 📚 数据导入

### 1. 准备数据导入脚本

```bash
# 安装Python依赖
sudo apt install python3-pip -y
pip3 install elasticsearch

### 2. 执行数据导入

```bash
# 先进行干运行，查看数据统计
python3 scripts/import_classics.py --dir /path/to/daizhigev20 --dry-run

# 正式导入数据（后台运行）
nohup python3 scripts/import_classics.py \
  --dir /path/to/daizhigev20 \
  --index chinese-classics \
  --batch-size 25 \
  > import.log 2>&1 &

# 监控导入进度
tail -f import.log

# 查看导入统计
watch -n 30 'curl -s "localhost:9200/chinese-classics/_count?pretty"'
```

### 3. 数据导入结果

预期导入结果：
- **总文档数**: 15,694个
- **索引大小**: 约10-15GB
- **导入时间**: 2-3小时
- **各藏分布**:
  - 佛藏: 5,135个文档
  - 史藏: 2,043个文档
  - 集藏: 1,948个文档
  - 道藏: 1,721个文档
  - 子藏: 1,463个文档
  - 其他各藏...

## 🌐 Web界面部署

### 1. 部署搜索页面

```bash
# 创建Web目录
sudo mkdir -p /var/www/html

# 保存搜索页面HTML文件
sudo cp web/search.html /var/www/html/

# 安装并启动Nginx
sudo apt install nginx -y
sudo systemctl start nginx
sudo systemctl enable nginx
```

### 2. 配置访问方式

**方法1: SSH隧道访问（推荐）**
```bash
# 在本地计算机运行
ssh -L 9200:localhost:9200 -L 80:localhost:80 username@server_ip

# 访问地址: http://localhost/search.html
```

**方法2: 直接访问（需谨慎配置防火墙）**
```bash
# 开放必要端口
sudo ufw allow 80
sudo ufw allow ssh

# 访问地址: http://server_ip/search.html
```

## 🔧 系统维护

### 日常监控

```bash
# 检查Elasticsearch状态
curl -X GET "localhost:9200/_cluster/health?pretty"

# 查看索引统计
curl -X GET "localhost:9200/_cat/indices?v"

# 监控系统资源
free -h
df -h
```

### 性能优化

```bash
# 查看慢查询日志
sudo tail -f /var/log/elasticsearch/text-search-cluster_index_search_slowlog.log

# 强制合并索引（提高查询性能）
curl -X POST "localhost:9200/chinese-classics/_forcemerge?max_num_segments=1"

# 清理缓存
curl -X POST "localhost:9200/_cache/clear"
```

### 备份与恢复

```bash
# 创建快照仓库
curl -X PUT "localhost:9200/_snapshot/backup_repo" -H 'Content-Type: application/json' -d'
{
  "type": "fs",
  "settings": {
    "location": "/var/lib/elasticsearch/backup"
  }
}'

# 创建快照
curl -X PUT "localhost:9200/_snapshot/backup_repo/snapshot_1" -H 'Content-Type: application/json' -d'
{
  "indices": "chinese-classics",
  "ignore_unavailable": true,
  "include_global_state": false
}'
```

## 🔍 使用示例

### API搜索示例

```bash
# 基本关键词搜索
curl -X GET "localhost:9200/chinese-classics/_search" -H 'Content-Type: application/json' -d'
{
  "query": {"match": {"content": "道德经"}},
  "size": 5
}'

# 按分类搜索
curl -X GET "localhost:9200/chinese-classics/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool": {
      "must": [{"match": {"content": "养生"}}],
      "filter": [{"term": {"collection": "医藏"}}]
    }
  }
}'

# 统计查询
curl -X GET "localhost:9200/chinese-classics/_search" -H 'Content-Type: application/json' -d'
{
  "aggs": {
    "by_collection": {
      "terms": {"field": "collection", "size": 10}
    }
  },
  "size": 0
}'
```

## 🚨 故障排除

### 常见问题

1. **服务启动失败**
   ```bash
   # 查看详细错误日志
   sudo journalctl -xeu elasticsearch.service
   sudo tail -f /var/log/elasticsearch/text-search-cluster.log
   ```

2. **内存不足**
   ```bash
   # 降低JVM堆内存
   echo -e "-Xms2g\n-Xmx2g" | sudo tee /etc/elasticsearch/jvm.options.d/heap.options
   sudo systemctl restart elasticsearch
   ```

3. **搜索无结果**
   ```bash
   # 检查索引是否存在
   curl -X GET "localhost:9200/_cat/indices?v"
   
   # 检查文档数量
   curl -X GET "localhost:9200/chinese-classics/_count?pretty"
   ```

4. **导入进程卡住**
   ```bash
   # 重启导入（会跳过已导入文档）
   pkill -f import_classics.py
   nohup python3 import_classics.py --index chinese-classics --batch-size 10 > restart.log 2>&1 &
   ```

### 日志位置

- **Elasticsearch日志**: `/var/log/elasticsearch/text-search-cluster.log`
- **系统服务日志**: `journalctl -u elasticsearch`
- **导入日志**: `import.log`（当前目录）

## 📊 性能参考

### 基准测试结果

**测试环境**: 15GB内存，4核CPU，SSD存储

- **索引大小**: 约12GB（15,694个文档）
- **内存使用**: 峰值8GB
- **查询响应时间**:
  - 简单关键词搜索: 50-200ms
  - 复杂查询: 200-500ms
  - 聚合查询: 500ms-2秒
- **并发支持**: 20-50个并发用户

## 🤝 贡献与支持

### 项目结构

```
project/
├── config/elasticsearch.yml    # ES配置文件
├── scripts/import_classics.py  # 数据导入脚本  
├── web/search.html             # Web搜索界面
└── DEPLOYMENT.md               # 本文档
```

### 基本信息

- 项目用途：基于「殆知阁」公开数据的中国古典文献全文搜索工具
- 技术栈：Elasticsearch + Python + HTML/JavaScript
- 数据规模：15,694个文档，约4.8GB原始数据

---

## 📝 更新日志

- **2025-08-24**: 初始版本部署完成
  - Elasticsearch 8.19.2 安装配置
  - 15,694个古典文献导入
  - Web搜索界面开发完成
  - 安全配置优化
- **2025-08-25**: 修正部分文字错误
  - 详情参见[Issues List](https://github.com/frankslin/daizhigev20/issues?q=is%3Aissue)

---

**🎉 恭喜！你现在拥有了一个功能完整的中国古典文献搜索引擎！**
